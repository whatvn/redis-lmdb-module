#include "redis.h"
#include <lmdb.h>
#include "redistolmdb.h"



#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
        "%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

static MDB_env *env;
struct sharedObjectsStruct shared;

int write_db(robj *key, msgpack_sbuffer *buf) {
    int rc = 0;
    MDB_val mdb_key, mdb_data;
    MDB_dbi dbi;
    MDB_txn *txn;
    mdb_key.mv_size = strlen((char *) key->ptr);
    mdb_key.mv_data = (char *) key->ptr;

    mdb_data.mv_size = buf->size;
    mdb_data.mv_data = buf->data;
    /*
    fprintf(stderr, "key: %s, value: %s", (char *) mdb_key.mv_data, (char *) mdb_data.mv_data);
     */
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    if (rc) {
        goto out;
    }
    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc) {
        goto out;
    }
    rc = mdb_put(txn, dbi, &mdb_key, &mdb_data, 0);
out:
    if(txn) {
        mdb_txn_commit(txn);
    }
    return rc;
}

int read_db(robj *key, MDB_val *val) {
    int rc = 0;
    MDB_val mdb_key;
    MDB_dbi dbi;
    MDB_txn *txn;
    //    static MDB_cursor *cursor;
    //    struct redisObject* key = c->argv[1];
    /*
    fprintf(stderr, "key: %s, size: %d\n", key->ptr, strlen(key->ptr));
     */
    mdb_key.mv_size = strlen((const char*) key->ptr);
    mdb_key.mv_data = key->ptr;
    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    if (rc) {
        goto out;
    }
    rc = mdb_dbi_open(txn, NULL, 0, &dbi);
    if (rc) {
        goto out;
    }
    rc = mdb_get(txn, dbi, &mdb_key, val);
out:
    if(txn) {
        mdb_txn_abort(txn);
    }
    return rc;
}

void lmdbgetCommand(redisClient *c) {
    int rc;
    if (c->argc != 2) {
        addReplyError(c, "lget command  takes 2 arguments");
        return;
    }
    MDB_val val;
    struct redisObject* key = c->argv[1];
    rc = read_db(key, &val);
    /*
    fprintf(stderr, "key: %s, size: %d\n", key->ptr, strlen(key->ptr));
     */

    if (rc == MDB_NOTFOUND) {
        fprintf(stderr, "key: %s not found\n", key->ptr);
        addReply(c, shared.nullbulk) ;
        return;
    } else if (rc == MDB_SUCCESS) {
        char msg[strlen((const char*) val.mv_data) + sizeof ("\r\n")];
        sprintf(msg, "+%s\r\n", (const char*) val.mv_data);
        //    sds rsp = sdsnew(msg);
        //    robj *o = createObject(REDIS_STRING, rsp);
        //    mdb_cursor_close(cursor);
        addReplyString(c, msg, sizeof (msg));
        return;
    }
    addReplyError(c, "error: mdb key cannot be retrieved at this time ");
    /*
    fprintf(stderr, "key: %s, value: %s", (const char*) mdb_key.mv_data,
            (const char*) mdb_data.mv_data);
     */
}

void lmdbsetCommand(redisClient *c) {
    int rc;

    if (c->argc != 3) {
        addReplyError(c, "lset command takes 2 arguments");
        return;
    }
    robj *key = c->argv[1];
    msgpack_sbuffer *buf = (msgpack_sbuffer*) c->argv[2]->ptr;
    rc = write_db(key, buf);
    /*
    fprintf(stderr, "key: %s, value: %s", (char *) mdb_key.mv_data, (char *) mdb_data.mv_data);
     */
    if (rc) {
        addReplyError(c, "Cannot set key");
    }
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
    robj *key = c->argv[1];
    msgpack_sbuffer *buf = (msgpack_sbuffer*) c->argv[2]->ptr;
    rc = write_db(key, buf);
    /*
    fprintf(stderr, "key: %s, value: %s", (char *) mdb_key.mv_data, (char *) mdb_data.mv_data);
     */
    if (rc) {
        addReplyError(c, "Cannot set key");
    }
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
    video_detail *vdu = NULL;

    msgpack_unpacked_init(&result);
    if (msgpack_unpacker_buffer_capacity(unp) < EACH_RECV_SIZE) {
        bool expanded = msgpack_unpacker_reserve_buffer(unp, 100);
        assert(expanded);
    }
    buf = msgpack_unpacker_buffer(unp);

    vdu = zmalloc(sizeof (video_detail));
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
            return NULL;
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
    //    {"lmdbget", lmdbmgetCommand, -2, "rF", 0, NULL, 1, 1, 1, 0, 0},
    {"lmdbset", lmdbsetCommand, -3, "wm", 0, NULL, 1, 1, 1, 0, 0},
    //    {"lmdbset", lmdbmsetCommand, -3, "wm", 0, NULL, 1, -1, 2, 0, 0},
    /* function from Redis */
    {0} /* Always end your command table with {0}
          * If you forget, you will be reminded with a segfault on load. */
};

