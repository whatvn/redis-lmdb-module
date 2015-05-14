#include "redis.h"
#include <lmdb.h>
#include "redistolmdb.h"



#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
        "%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

static MDB_env *env;

void lmdbgetCommand(redisClient *c) {
    int rc;
    MDB_val mdb_key, mdb_data;
    MDB_dbi dbi;
    MDB_txn *txn;
    //    static MDB_cursor *cursor;
    if (c->argc != 2) {
        addReplyError(c, "lget command  takes 2 arguments");
        return;
    }
    struct redisObject* key = c->argv[1];
    /*
    fprintf(stderr, "key: %s, size: %d\n", key->ptr, strlen(key->ptr));
     */
    mdb_key.mv_size = strlen((const char*) key->ptr);
    mdb_key.mv_data = key->ptr;
    E(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));

    if (rc) {
        addReplyError(c, "error: mdb key cannot be retrieved at this time ");
        return;
    }

    rc = mdb_get(txn, dbi, &mdb_key, &mdb_data);
    if (rc == MDB_NOTFOUND) {
        //        mdb_cursor_close(cursor);
        mdb_txn_abort(txn);
        addReplyError(c, "(nil)");
        return;
    }
    /*
    fprintf(stderr, "key: %s, value: %s", (const char*) mdb_key.mv_data,
            (const char*) mdb_data.mv_data);
     */
    char msg[strlen((const char*) mdb_data.mv_data) + sizeof ("\r\n")];
    sprintf(msg, "+%s\r\n", (const char*) mdb_data.mv_data);
    //    sds rsp = sdsnew(msg);
    //    robj *o = createObject(REDIS_STRING, rsp);
    //    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    addReplyString(c, msg, sizeof (msg));
    //    addReply(c, o);
}

void lmdbsetCommand(redisClient *c) {
    int rc;
    MDB_val mdb_key, mdb_data;
    MDB_dbi dbi;
    MDB_txn *txn;
    //
    //    if (c->argc != 2) {
    //        addReplyError(c, "lset command takes 2 arguments");
    //        return;
    //    }

    struct redisObject* key = c->argv[1];
    struct redisObject* val = c->argv[2];

    mdb_key.mv_size = strlen((char *) key->ptr);
    mdb_key.mv_data = (char *) key->ptr;

    mdb_data.mv_size = strlen((char *) val->ptr);
    mdb_data.mv_data = (char *) val->ptr;
    /*
    fprintf(stderr, "key: %s, value: %s", (char *) mdb_key.mv_data, (char *) mdb_data.mv_data);
     */
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));
    if (rc) {
        addReplyError(c, "error: mdb cannot open transaction at this time ");
        return;
    }
    E(mdb_put(txn, dbi, &mdb_key, &mdb_data, 0));
    if (rc) {
        addReplyError(c, "error: Cannot put key");
        return;
    }
    mdb_txn_commit(txn);
    if (rc) {
        addReplyError(c, "error: Cannot commit put transaction");
        return;
    }
    //    E(mdb_txn_commit(txn));
    sds rsp = sdsnew("+OK\r\n");
    robj *o = createObject(REDIS_STRING, rsp);
    addReply(c, o);
}

void lmdbmgetCommand(redisClient *c) {
    if (c->argc % 2 == 1) {
        addReplyError(c, "lmdbget command: wrong number of arguments");
        return;
    }
}

void lmdbmsetCommand(redisClient *c) {

    if ((c->argc - 3) % 2 != 0) {
        addReplyError(c, "wrong number of arguments");
    }
    int rc;
    MDB_val mdb_key, mdb_data;
    MDB_dbi dbi;
    MDB_txn *txn;
    robj *cmd = c->argv[0];
    robj *key = c->argv[1];
    msgpack_sbuffer *val = (msgpack_sbuffer) c->argv[2]->ptr;



    mdb_key.mv_size = strlen((char *) key->ptr);
    mdb_key.mv_data = (char *) key->ptr;

    mdb_data.mv_size = val->size;
    mdb_data.mv_data = val->data;
    /*
    fprintf(stderr, "key: %s, value: %s", (char *) mdb_key.mv_data, (char *) mdb_data.mv_data);
     */
    E(mdb_txn_begin(env, NULL, 0, &txn));
    E(mdb_dbi_open(txn, NULL, 0, &dbi));
    if (rc) {
        addReplyError(c, "error: mdb cannot open transaction at this time ");
        return;
    }
    E(mdb_put(txn, dbi, &mdb_key, &mdb_data, 0));
    if (rc) {
        addReplyError(c, "error: Cannot put key");
        return;
    }
    mdb_txn_commit(txn);
    if (rc) {
        addReplyError(c, "error: Cannot commit put transaction");
        return;
    }
    //    E(mdb_txn_commit(txn));
    sds rsp = sdsnew("+OK\r\n");
    robj *o = createObject(REDIS_STRING, rsp);
    addReply(c, o);

}

void receiver_init(receiver *r, video_detail *v) {
    msgpack_packer pk;
    msgpack_sbuffer_init(&r->sbuf);
    msgpack_packer_init(&pk, &r->sbuf, msgpack_sbuffer_write);
    msgpack_pack_array(&pk, 3);
    msgpack_pack_long(&pk, v->video_id);
    msgpack_pack_str(&pk, strlen(v->video_thumb_url) + 1);
    msgpack_pack_str_body(&pk, v->video_thumb_url, strlen(v->video_thumb_url) + 1);
    msgpack_pack_str(&pk, strlen(v->video_title) + 1);
    msgpack_pack_str_body(&pk, v->video_title, strlen(v->video_title) + 1);
    r->rest = r->sbuf.size;
}

size_t receiver_recv(receiver *r, char* buf, size_t try_size) {
    size_t off = r->sbuf.size - r->rest;

    size_t actual_size = try_size;
    if (actual_size > r->rest) actual_size = r->rest;

    memcpy(buf, r->sbuf.data + off, actual_size);
    r->rest -= actual_size;

    return actual_size;
}

video_detail *unpack(receiver* r) {
    /* buf is allocated by unpacker. */
    msgpack_unpacker* unp = msgpack_unpacker_new(100);
    msgpack_unpacked result;
    msgpack_unpack_return ret;
    char* buf;
    size_t recv_len;
    int recv_count = 0;
    int i = 0;
    video_detail *vdu = NULL;

    msgpack_unpacked_init(&result);
    if (msgpack_unpacker_buffer_capacity(unp) < EACH_RECV_SIZE) {
        bool expanded = msgpack_unpacker_reserve_buffer(unp, 100);
        assert(expanded);
    }
    buf = msgpack_unpacker_buffer(unp);

    vdu = malloc(sizeof (video_detail));
    recv_len = receiver_recv(r, buf, EACH_RECV_SIZE);
    msgpack_unpacker_buffer_consumed(unp, recv_len);


    while (recv_len > 0) {
        printf("receive count: %d %zd bytes received.\n", recv_count++, recv_len);
        ret = msgpack_unpacker_next(unp, &result);
        while (ret == MSGPACK_UNPACK_SUCCESS) {
            msgpack_object obj = result.data;
            /*
            printf("array size: %d\n", result.data.via.array.size);
             */
            vdu->video_id = obj.via.array.ptr[0].via.u64;
            /*
            printf("before unpack: %s\n", obj.via.array.ptr[1].via.str.ptr);
             */
            vdu->video_thumb_url = strdup(obj.via.array.ptr[1].via.str.ptr);
            vdu->video_title = strdup(obj.via.array.ptr[2].via.str.ptr);
            /* Use obj. */
            /*
            printf("Object no %d:\n", ++i);
            printf("id: %ld, thumb: %s, url: %s\n", vdu->video_id, vdu->video_thumb_url, vdu->video_title);
            msgpack_object_print(stdout, obj);
            printf("\n");
             */
            /* If you want to allocate something on the zone, you can use zone. */
            /* msgpack_zone* zone = result.zone; */
            /* The lifetime of the obj and the zone,  */
            ret = msgpack_unpacker_next(unp, &result);
        }
        if (ret == MSGPACK_UNPACK_PARSE_ERROR) {
            printf("The data in the buf is invalid format.\n");
            msgpack_unpacked_destroy(&result);
            return;
        }
        if (msgpack_unpacker_buffer_capacity(unp) < EACH_RECV_SIZE) {
            bool expanded = msgpack_unpacker_reserve_buffer(unp, 100);
            assert(expanded);
        }
        buf = msgpack_unpacker_buffer(unp);
        recv_len = receiver_recv(r, buf, 4);
        msgpack_unpacker_buffer_consumed(unp, recv_len);
    }
    msgpack_unpacked_destroy(&result);
    return vdu;
}

//int main(void) {
//    receiver r;
//    video_detail *vd = NULL;
//    vd = malloc(sizeof(video_detail));
//    vd->video_id = 1000;
//    vd->video_thumb_url = "hihihehe";
//    vd->video_title = "veryverylongveryverylongveryverylongveryverylongveryverylongveryverylong";
//    receiver_init(&r, vd);
//    unpack(&r);
//
//    return 0;
//}

void *load() {
    int rc;
    E(mdb_env_create(&env));
    E(mdb_env_set_mapsize(env, 1 * 1024 * 1024LL));
    E(mdb_env_set_maxreaders(env, 1024));
    E(mdb_env_open(env, "./testdb", MDB_WRITEMAP | MDB_MAPASYNC, 0664));
    fprintf(stderr, "%s: Loaded at %d\n", __FILE__, __LINE__);
    return NULL;
}

void cleanup(void *privdata) {
    //    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    fprintf(stderr, "%s: Cleaning up at %d\n", __FILE__, __LINE__);
}




struct redisModule redisModuleDetail = {
    REDIS_MODULE_COMMAND, /* Tell Dynamic Redis our module provides commands */
    REDIS_VERSION, /* Provided by redis.h */
    "0.0001", /* Version of this module (only for reporting) */
    "ab.tech.lmdb.redis", /* Unique name of this module */
    load, /* Load function pointer (optional) */
    cleanup /* Cleanup function pointer (optional) */
};

struct redisCommand redisCommandTable[] = {
    {"lmdbget", lmdbgetCommand, 2, "rF", 0, NULL, 1, 1, 1, 0, 0},
    {"lmdbget", lmdbmgetCommand, -2, "rF", 0, NULL, 1, 1, 1, 0, 0},
    {"lmdbset", lmdbsetCommand, -3, "wm", 0, NULL, 1, 1, 1, 0, 0},
    {"lmdbset", lmdbmsetCommand, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
    /* function from Redis */
    {0} /* Always end your command table with {0}
          * If you forget, you will be reminded with a segfault on load. */
};

