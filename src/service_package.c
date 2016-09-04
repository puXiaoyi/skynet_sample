#include "skynet.h"
#include "skynet_socket.h"
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#define TIMEOUT "1000"	// 10s

// 网络消息
struct response {
	size_t sz;	// 解包后的数据长度
	void * msg;	// 解包后的数据内容
};

// 内部消息
struct request {
	uint32_t source; // 内部服务id
	int session;
};

// 队列结构
struct queue {
	int cap; // 内容容量
	int sz;	// 内容长度
	int head; // 头索引
	int tail; // 尾索引
	char * buffer; // 数据缓冲区，分配内存大小为cap * sz
};

// 服务结构
struct package {
	uint32_t manager;	// 创建c服务的lua服务句柄
	int fd;	// 管理的fd
	int heartbeat; // 心跳标识，初始-1，启动定时器后置为0
	int recv; // 数据标识，初始0，每接收一个网络消息+1
	int init; // 初始标识
	int closed;	// 关闭标识

	int header_sz; // 头信息标识 0 完整 1 不完整
	uint8_t header[2]; // 两字节头信息
	int uncomplete_sz; // 正在打包的数据包长度， -1 没有正在打包的数据包
	struct response uncomplete;	// 正在打包的数据包

	struct queue request;	// 内部请求队列
	struct queue response;	// 网络消息队列
};

static void
queue_init(struct queue *q, int sz) {
	q->head = 0;
	q->tail = 0;
	q->sz = sz;
	q->cap = 4;
	q->buffer = skynet_malloc(q->cap * q->sz);
}

static void
queue_exit(struct queue *q) {
	skynet_free(q->buffer);
	q->buffer = NULL;
}

static int
queue_empty(struct queue *q) {
	return q->head == q->tail;
}

static int
queue_pop(struct queue *q, void *result) {
	if (q->head == q->tail) {
		return 1;
	}
	memcpy(result, q->buffer + q->head * q->sz, q->sz);
	q->head++;
	if (q->head >= q->cap)
		q->head = 0;
	return 0;
}

static void
queue_push(struct queue *q, const void *value) {
	void * slot = q->buffer + q->tail * q->sz;
	++q->tail;
	if (q->tail >= q->cap)
		q->tail = 0;
	if (q->head == q->tail) {
		// full
		assert(q->sz > 0);
		int cap = q->cap * 2;
		char * tmp = skynet_malloc(cap * q->sz);
		int i;
		int head = q->head;
		for (i=0;i<q->cap;i++) {
			memcpy(tmp + i * q->sz, q->buffer + head * q->sz, q->sz);
			++head;
			if (head >= q->cap) {
				head = 0;
			}
		}
		skynet_free(q->buffer);
		q->head = 0;
		slot = tmp + (q->cap-1) * q->sz;
		q->tail = q->cap;
		q->cap = cap;
		q->buffer = tmp;
	}
	memcpy(slot, value, q->sz);
}

static int
queue_size(struct queue *q) {
	if (q->head > q->tail) {
		return q->tail + q->cap - q->head;
	}
	return q->tail - q->head;
}

static void
service_exit(struct skynet_context *ctx, struct package *P) {
	// report manager
	P->closed = 1;
	while (!queue_empty(&P->request)) {
		struct request req;
		queue_pop(&P->request, &req);
		skynet_send(ctx, 0, req.source, PTYPE_ERROR, req.session, NULL, 0);
	}
	while (!queue_empty(&P->response)) {
		// drop the message
		struct response resp;
		queue_pop(&P->response, &resp);
		skynet_free(resp.msg);
	}
	skynet_send(ctx, 0, P->manager, PTYPE_TEXT, 0, "CLOSED", 6);
	skynet_command(ctx, "EXIT", NULL);
}

static void
report_info(struct skynet_context *ctx, struct package *P, int session, uint32_t source) {
	int uncomplete;
	int uncomplete_sz;
	if (P->header_sz != 0) {
		// 数据头信息不全，只接收了高位信息
		uncomplete = -1;
		uncomplete_sz = 0;
	} else if (P->uncomplete_sz == 0) {
		// 数据头信息为零，空内容数据包
		uncomplete = 0;
		uncomplete_sz = 0;
	} else {
		// 正在打包的数据包
		uncomplete = P->uncomplete_sz;	// 未打包的剩余数据长度
		uncomplete_sz = P->uncomplete.sz;	// 需打包的数据总长度
	}
	char tmp[128];
	int n = sprintf(tmp,"req=%d resp=%d uncomplete=%d/%d", queue_size(&P->request), queue_size(&P->response),uncomplete,uncomplete_sz);
	skynet_send(ctx, 0, source, PTYPE_RESPONSE, session, tmp, n);
}

static void
command(struct skynet_context *ctx, struct package *P, int session, uint32_t source, const char *msg, size_t sz) {
	switch (msg[0]) {
	case 'R':
		// request a package
		if (P->closed) {
			skynet_send(ctx, 0, source, PTYPE_ERROR, session, NULL, 0);
			break;
		}
		// 内部请求一个网络数据包
		if (!queue_empty(&P->response)) {
			// 如果就绪数据包队列不为空，弹出一个发送给请求服务方
			assert(queue_empty(&P->request));
			struct response resp;
			queue_pop(&P->response, &resp);
			skynet_send(ctx, 0, source, PTYPE_RESPONSE | PTYPE_TAG_DONTCOPY, session, resp.msg, resp.sz);
		} else {
			// 如果没有就绪的数据包，生成请求压入内部请求队列
			struct request req;
			req.source = source;
			req.session = session;
			queue_push(&P->request, &req);
		}
		break;
	case 'K':
		// shutdown the connection
		skynet_socket_shutdown(ctx, P->fd);
		break;
	case 'I':
		report_info(ctx, P, session, source);
		break;
	default:
		// invalid command
		skynet_error(ctx, "Invalid command %.*s", (int)sz, msg);
		skynet_send(ctx, 0, source, PTYPE_ERROR, session, NULL, 0);
		break;
	};
}

// 一条新的网络消息
static void
new_message(struct package *P, const uint8_t *msg, int sz) {
	++P->recv;
	for (;;) {
		if (P->uncomplete_sz >= 0) {
			// 如果有正在打包的数据包
			if (sz >= P->uncomplete_sz) {
				// 如果未读网络数据长度大于正在打包的数据包长度，填充正在打包的数据包并压入队列，“粘包”
				memcpy(P->uncomplete.msg + P->uncomplete.sz - P->uncomplete_sz, msg, P->uncomplete_sz);
				// 数据指针偏移，数据长度更新，清理正在打包标识
				msg += P->uncomplete_sz;
				sz -= P->uncomplete_sz;
				queue_push(&P->response, &P->uncomplete);
				P->uncomplete_sz = -1;
			} else {
				// 如果未读网络数据长度小于正在打包的数据包长度，全部填充为正在打包的数据包，等待接收下一次网络数据，“半包”
				memcpy(P->uncomplete.msg + P->uncomplete.sz - P->uncomplete_sz, msg, sz);
				P->uncomplete_sz -= sz;
				return;
			}
		}

		if (sz <= 0)
			// 没有多余的网络数据可以处理
			return;

		if (P->header_sz == 0) {
			if (sz == 1) {
				// 如果数据只有1个字节，保存高位头信息，header_sz置为1，等待下次循环接收低位头信息数据内容，“半包”
				P->header[0] = msg[0];
				P->header_sz = 1;
				return;
			}
			// 保存两字节头信息，并做数据指针偏移
			P->header[0] = msg[0];
			P->header[1] = msg[1];
			msg+=2;
			sz-=2;
		} else {
			// 上次只收到高位头信息，继续接收低位头信息及数据内容
			assert(P->header_sz == 1);
			// 保存低位头信息，并做数据指针偏移
			P->header[1] = msg[0];
			P->header_sz = 0;
			++msg;
			--sz;
		}
		// 根据两字节头计算数据包长度，分配相应大小的内存空间
		P->uncomplete.sz = P->header[0] * 256 + P->header[1];
		P->uncomplete.msg = skynet_malloc(P->uncomplete.sz);
		P->uncomplete_sz = P->uncomplete.sz;
	}
}

// 发送内部消息数据 网络->内部
static void
response(struct skynet_context *ctx, struct package *P) {
	while (!queue_empty(&P->request)) {
		if (queue_empty(&P->response)) {
			break;
		}
		struct request req;
		struct response resp;
		queue_pop(&P->request, &req);
		queue_pop(&P->response, &resp);
		skynet_send(ctx, 0, req.source, PTYPE_RESPONSE | PTYPE_TAG_DONTCOPY, req.session, resp.msg, resp.sz);
	}
}

static void
socket_message(struct skynet_context *ctx, struct package *P, const struct skynet_socket_message * smsg) {
	switch (smsg->type) {
	case SKYNET_SOCKET_TYPE_CONNECT:
		if (P->init == 0 && smsg->id == P->fd) {
			// 通知lua管理服务，唤醒挂起协程
			skynet_send(ctx, 0, P->manager, PTYPE_TEXT, 0, "SUCC", 4);
			P->init = 1;
		}
		break;
	case SKYNET_SOCKET_TYPE_CLOSE:
	case SKYNET_SOCKET_TYPE_ERROR:
		if (P->init == 0 && smsg->id == P->fd) {
			skynet_send(ctx, 0, P->manager, PTYPE_TEXT, 0, "FAIL", 4);
			P->init = 1;
		}
		if (smsg->id != P->fd) {
			skynet_error(ctx, "Invalid fd (%d), should be (%d)", smsg->id, P->fd);
		} else {
			// todo: log when SKYNET_SOCKET_TYPE_ERROR
			response(ctx, P);
			service_exit(ctx, P);
		}
		break;
	case SKYNET_SOCKET_TYPE_DATA:
		// 收到一条新的网络数据，处理成内部消息数据包
		new_message(P, (const uint8_t *)smsg->buffer, smsg->ud);
		skynet_free(smsg->buffer);
		// 发送内部消息数据包给发起请求的内部服务
		response(ctx, P);
		break;
	case SKYNET_SOCKET_TYPE_WARNING:
		skynet_error(ctx, "Overload on %d", P->fd);
		break;
	default:
		// ignore
		break;
	}
}

// 心跳检测
static void
heartbeat(struct skynet_context *ctx, struct package *P) {
	if (P->recv == P->heartbeat) {
		if (!P->closed) {
			skynet_socket_shutdown(ctx, P->fd);
			skynet_error(ctx, "timeout %d", P->fd);
		}
	} else {
		P->heartbeat = P->recv = 0;
		skynet_command(ctx, "TIMEOUT", TIMEOUT);
	}
}

// 发送socket数据，内部->网络
static void
send_out(struct skynet_context *ctx, struct package *P, const void *msg, size_t sz) {
	if (sz > 0xffff) {
		skynet_error(ctx, "package too long (%08x)", (uint32_t)sz);
		return;
	}
	// 按2字节头打包数据后通过socket发送
	uint8_t *p = skynet_malloc(sz + 2);
	p[0] = (sz & 0xff00) >> 8;
	p[1] = sz & 0xff;
	memcpy(p+2, msg, sz);
	skynet_socket_send(ctx, P->fd, p, sz+2);
}

static int
message_handler(struct skynet_context * ctx, void *ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	struct package *P = ud;
	switch (type) {
	case PTYPE_TEXT:
		// lua层发送的文本指令，比如socket_proxy.lua中的proxy.read(fd)方法，发送指令‘R’
		command(ctx, P, session, source, msg, sz);
		break;
	case PTYPE_CLIENT:
		// lua层发送socket数据，socket_proxy.lua中的proxy.write(fd, msg, sz)
		send_out(ctx, P, msg, sz);
		break;
	case PTYPE_RESPONSE:
		// It's timer
		heartbeat(ctx, P);
		break;
	case PTYPE_SOCKET:
		// 网络socket消息
		socket_message(ctx, P, msg);
		break;
	case PTYPE_ERROR:
		// ignore error
		break;
	default:
		if (session > 0) {
			// unsupport type, raise error
			skynet_send(ctx, 0, source, PTYPE_ERROR, session, NULL, 0);
		}
		break;
	}
	return 0;
}

struct package *
package_create(void) {
	struct package * P = skynet_malloc(sizeof(*P));
	memset(P, 0, sizeof(*P));
	P->heartbeat = -1;
	P->uncomplete_sz = -1;
	// 初始化请求和回应队列
	queue_init(&P->request, sizeof(struct request));
	queue_init(&P->response, sizeof(struct response));
	return P;
}

void
package_release(struct package *P)
	// 清理请求和回应队列
	queue_exit(&P->request);
	queue_exit(&P->response);
	skynet_free(P);
}

int
package_init(struct package * P, struct skynet_context *ctx, const char * param) {
	int n = sscanf(param ? param : "", "%u %d", &P->manager, &P->fd);
	if (n != 2 || P->manager == 0 || P->fd == 0) {
		skynet_error(ctx, "Invalid param [%s]", param);
		return 1;
	}
	// 启动socket
	skynet_socket_start(ctx, P->fd);
	skynet_socket_nodelay(ctx, P->fd);
	heartbeat(ctx, P);
	// 注册回调函数
	skynet_callback(ctx, P, message_handler);

	return 0;
}
