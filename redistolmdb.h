/* 
 * File:   redistolmdb.h
 * Author: hungnguyen
 *
 * Created on May 14, 2015, 5:46 PM
 */
#include <msgpack.h>
#include <stdio.h>
#include <assert.h>

typedef struct __attribute__((packed)) {
    long video_id;
    unsigned char *video_title;
    unsigned char *video_thumb_url;
}
video_detail;

typedef struct receiver {
    msgpack_sbuffer sbuf;
    size_t rest;
} receiver;

#define EACH_RECV_SIZE 8

#ifndef REDISTOLMDB_H
#define	REDISTOLMDB_H

#ifdef	__cplusplus
extern "C" {
#endif

void lmdbgetCommand(redisClient *c);
void lmdbsetCommand(redisClient *c);
void lmdbmsetCommand(redisClient *c);
void receiver_init(receiver *r, video_detail *v) ;
size_t receiver_recv(receiver *r, char* buf, size_t try_size);
video_detail  *unpack(receiver* r) ;


#ifdef	__cplusplus
}
#endif

#endif	/* REDISTOLMDB_H */

