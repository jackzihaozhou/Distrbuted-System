/*
 *  Name: Zihao ZHOU ID:zihaozho
 */

/*
 *  a client of simple rpc implementation
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <err.h>
#include<errno.h>
#define MAXMSGLEN 1000100
#define MAXSTORE 1000100
#define MAXPARA 8
#include <dirtree.h>

int (*orig_open)(const char *pathname, int flags, ...);
int (*orig_close)(int fd);
ssize_t (*orig_write)(int fd, const void *buf, size_t count);
ssize_t (*orig_read)(int fd, void *buf, size_t count);
off_t (*orig_lseek)(int fd, off_t offset, int whence);
int (*orig_stat)(int ver, const char * path, struct stat * stat_buf);
int (*orig_unlink)(const char *pathname);
ssize_t (*orig_getdirentries)(int fd, char *buf, size_t nbytes, off_t *basep);
struct dirtreenode* (*orig_getdirtree)( const char *path );
void (*orig_freedirtree)( struct dirtreenode* dt );
void split(char *src,const char *separator,char **dest,int cutnum);
int client(char *msg);
int client_open(char *msg, int slength);
ssize_t client_read(char* send,char *buf,int slength);
int client_close(char *msg,int slength);
ssize_t client_write(char *buf,int slength);
off_t client_lseek(char *msg,int slength);
int client_xstat(char *msg,struct stat * stat_buf,int slength);
ssize_t client_getdirentries(char *msg, char *buf,off_t *basep,int slength);
int client_unlink(char *msg,int slength);

int sockfd = 0;

// The following function is replacement for the original functions

//open interposition
int open(const char *pathname, int flags, ...) {
    int fd = 0;
    int op;
    char temp[MAXMSGLEN]={0};
    char send[MAXMSGLEN]={0};
	mode_t m=0;
	int length;
	if (flags & O_CREAT) {
		va_list a;
		va_start(a, flags);
		m = va_arg(a, mode_t);
		va_end(a);
	}
	if(m == 0){
        op=1;
        sprintf(temp, "%8d%s %d",op,pathname,flags);
        length = strlen(temp)+8;
        sprintf(send, "%8d%s",length,temp);
        fd = client_open(send,length);

	}
	else{
        op=2;
        sprintf(temp, "%8d%s %d %d",op,pathname,flags,m);
        length = strlen(temp)+8;
        sprintf(send, "%8d%s",length,temp);
        fd = client_open(send,length);
	}
	return (fd);
}

//close interposition
int close(int fd){
	char temp[MAXMSGLEN]={0};
	char send[MAXMSGLEN]={0};
	int closeret = 0;
	int op =4;
	int length;
	if(fd>=5000){
        sprintf(temp, "%8d%8d",op,fd);
        length = strlen(temp)+8;
        sprintf(send, "%8d%s",length,temp);
        closeret = client_close(send,length);
	}
    else{
        closeret = orig_close(fd);
    }
	return (closeret);
}
//write interposition
ssize_t write(int fd, const void *buf, size_t count){

    char bufsend[MAXSTORE+1]={0};
    ssize_t retssize = 0;
    int op =5;
    int length;
    if(fd>=5000){
        length = 32 + count;
        sprintf(bufsend, "%8d%8d%8d%8lu",length,op,fd,count);
        memcpy(&bufsend[32],buf,count);
        retssize = client_write(bufsend,length);
    }
    else{
        retssize = orig_write(fd,buf,count);
    }
	return(retssize);
}
// read interposition
ssize_t read(int fd, void *buf, size_t count){
	char send[MAXMSGLEN+1]={0};
	int op = 3;
    ssize_t readret = 0;
    int length = 32;
    if(fd>=5000){
        sprintf(send, "%8d%8d%8d%8lu",length,op,fd,count);
        readret = client_read(send,buf,length);
    }
    else{
        readret = orig_read(fd,buf,count);
    }
	return readret;
}
//lseek interposition
off_t lseek(int fd, off_t offset, int whence){
    off_t lseekret;
    int op =6;
    char send[MAXMSGLEN+1]={0};
    int length = 40;
    if(fd>=5000){
        sprintf(send, "%8d%8d%8d%8ld%8d",length,op,fd,offset,whence);
        lseekret = client_lseek(send,length);
    }
    else{
        lseekret = orig_lseek(fd,offset,whence);
    }
    return lseekret;
}
//stat interposition
int __xstat(int ver, const char * path, struct stat * stat_buf){
    int op =7;
    int xstatret;
    char send[MAXMSGLEN+1]={0};
    char temp[MAXMSGLEN+1]={0};
    int int_stat_buf = (int)stat_buf;
    sprintf(temp, "%8d%s %d",op,path,ver);
    int length = strlen(temp)+8;
    sprintf(send, "%8lu%s",strlen(temp)+8,temp);
    xstatret = client_xstat(send,stat_buf,length);
    return xstatret;
}
//unlink interposition
int unlink(const char *pathname){
    int length;
    int op=9;
    size_t path_length = strlen(pathname);
    char send[MAXMSGLEN+1]={0};
    int ret;
    length = (int)path_length + 16;
    sprintf(send, "%8d%8d%s",length,op,pathname);
    ret = client_unlink(send,length);
    return (ret);
}
//getdirentries interposition
ssize_t getdirentries(int fd, char *buf, size_t nbytes , off_t *basep){
    int op=8;
    int length = 40;
    ssize_t retssize = 0;
    char send[MAXMSGLEN+1]={0};
    if(fd>=5000){
        sprintf(send, "%8d%8d%8d%8lu%8ld",length,op,fd,nbytes,*basep);
        retssize = client_getdirentries(send,buf,basep,length);
    }
    else{
        retssize = orig_getdirentries(fd,buf,nbytes,basep);
    }
    return retssize;
}
//The getdirtree and freedirtree interpositions are not completed
struct dirtreenode* getdirtree( const char *path){
    client("getdirtree");
    return orig_getdirtree(path);
}

void freedirtree( struct dirtreenode* dt ){
    client("freedirtree");
    return orig_freedirtree(dt);
}
/*
 * init function:
 * 1. set function pointer orig_open to point to the original open function
 * 2. set up network
 */
void _init(void) {
	// set function pointer orig_open to point to the original open function
	orig_open = dlsym(RTLD_NEXT, "open");
	orig_close = dlsym(RTLD_NEXT, "close");
	orig_write = dlsym(RTLD_NEXT, "write");
	orig_read = dlsym(RTLD_NEXT, "read");
	orig_lseek = dlsym(RTLD_NEXT, "lseek");
	orig_stat = dlsym(RTLD_NEXT, "__xstat");
	orig_unlink = dlsym(RTLD_NEXT, "unlink");
	orig_getdirentries = dlsym(RTLD_NEXT, "getdirentries");
	orig_getdirtree = dlsym(RTLD_NEXT, "getdirtree");
	orig_freedirtree = dlsym(RTLD_NEXT, "freedirtree");

	char *serverip;
	char *serverport;
	unsigned short port;
	int rv;
	struct sockaddr_in srv;
	// Get environment variable indicating the ip address of the server
	serverip = getenv("server15440");
	if (serverip) {
	}
	else {
		serverip = "127.0.0.1";
	}

	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport){
	}
	else {
		serverport = "15440";
	}
	port = (unsigned short)atoi(serverport);
	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd<0) err(1, 0);

	// setup address structure to point to server
	memset(&srv, 0, sizeof(srv));
	srv.sin_family = AF_INET;
	srv.sin_addr.s_addr = inet_addr(serverip);
	srv.sin_port = htons(port);

	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);
}

//after finish rpc and close this program, close socket
void _fini(void) {
    orig_close(sockfd);
}

//send command "open" to server and receive return parameters
int client_open(char *msg,int slength){
	send(sockfd, msg, slength, 0);

    char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char openfd_buf[9]={0};

	int openfd;
	int length;
    int rv = 0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }

    }

    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(openfd_buf,&storebuf[16], 8);
    openfd = atoi(openfd_buf);
	return openfd;
}

//send command "read" to server and receive return parameters
ssize_t client_read(char *msg,char *buf,int slength) {

	send(sockfd, msg, slength, 0);
    char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char retssize_buf[9]={0};

	ssize_t retssize = 0;
	int length = MAXMSGLEN;
    int rv = 0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }

    }
    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(retssize_buf,&storebuf[16], 8);
    retssize = atol(retssize_buf);
    if(retssize>0){
        memcpy(buf,&storebuf[24], (size_t)retssize);
    }
	return retssize;
}

//send command "write" to server and receive return parameters
ssize_t client_write(char *msg,int slength){
	send(sockfd, msg, slength, 0);

    char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char retssize_buf[9]={0};


	ssize_t retssize = 0;
	int length = MAXMSGLEN;
    int rv = 0,rv_total = 0;



    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }

    }
    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(retssize_buf,&storebuf[16], 8);
    retssize = atol(retssize_buf);
	return retssize;
}

//send command "lseek" to server and receive return parameters
off_t client_lseek(char *msg,int slength){
    off_t lseekret;
    int rv,rv_total = 0;
    int length;

    char storebuf[MAXSTORE+1]={0};
    char length_buf[9]={0};
    char buf_errno[9]={0};
    char buf_lseekret[9]={0};

    send(sockfd, msg, slength, 0);


    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }
    }
    memcpy(buf_errno,&storebuf[8], 8);
    memcpy(buf_lseekret,&storebuf[16], 8);
    errno= atoi(buf_errno);
    lseekret = atol(buf_lseekret);
    return(lseekret);
}

//send command "stat" to server and receive return parameters
int client_xstat(char *msg, struct stat * stat_buf,int slength){

	send(sockfd, msg, slength, 0);
    char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char xstatret_buf[9]={0};


	int xstatret = 0;
	int length = MAXMSGLEN;
    int rv = 0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }
    }

    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(xstatret_buf,&storebuf[16], 8);
    xstatret = atol(xstatret_buf);
    memcpy(stat_buf,&storebuf[24], (size_t)(length-24));
	return xstatret;


}
//send command "getdirentries" to server and receive return parameters
ssize_t client_getdirentries(char *msg,char *buf, off_t *basep,int slength){

    send(sockfd, msg, slength, 0);
    char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char ret_buf[9]={0};
    char basep_buf[9] = {0};
    int int_basep;

	int ret ;
	int length = MAXMSGLEN;
    int rv = 0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }
    }
    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(ret_buf,&storebuf[16], 8);
    ret = atoi(ret_buf);
    memcpy(basep_buf,&storebuf[24], 8);
    int_basep = atoi(basep_buf);
    *basep = int_basep;
    if(length>32){
        memcpy(buf,&storebuf[32], (size_t)(length-32));
    }
	return ((ssize_t)ret);

}

//send command "unlink" to server and receive return parameters
int client_unlink(char *msg,int slength){

	send(sockfd, msg, slength, 0);

    int ret,length;
	char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char ret_buf[9]={0};
    int rv=0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }
    }
    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(ret_buf,&storebuf[16], 8);
    ret = atoi(ret_buf);
    return(ret);

}

//send command "close" to server and receive return parameters
int client_close(char *msg,int slength){

	send(sockfd, msg, slength, 0);	// send message; should check return value

    int ret,length;
	char storebuf[MAXSTORE]={0};
	char length_buf[9]={0};
	char errno_buf[9]={0};
	char ret_buf[9]={0};
    int rv=0,rv_total = 0;

    while(1){
        rv = recv(sockfd, &storebuf[rv_total], MAXMSGLEN, 0);
        memcpy(length_buf,storebuf, 8);
        length = atoi(length_buf);
        if(rv<=0){
            break;
        }
        rv_total = rv_total + rv;
        if(rv_total>=length){
            break;
        }
    }

    memcpy(errno_buf,&storebuf[8], 8);
    errno = atoi(errno_buf);
    memcpy(ret_buf,&storebuf[16], 8);
    ret = atoi(ret_buf);
    return(ret);
}

int client(char *msg) {
	char *serverip;
	char *serverport;
	unsigned short port;
	char buf[MAXMSGLEN+1]={0};
	int sockfd, rv;
	struct sockaddr_in srv;
	serverip = getenv("server15440");
	if (serverip) {
	}
	else {
		serverip = "127.0.0.1";
	}
	serverport = getenv("serverport15440");
	if (serverport){
	}
	else {
		serverport = "15440";
	}
	port = (unsigned short)atoi(serverport);
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd<0) err(1, 0);
	memset(&srv, 0, sizeof(srv));
	srv.sin_family = AF_INET;
	srv.sin_addr.s_addr = inet_addr(serverip);
	srv.sin_port = htons(port);
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);
	send(sockfd, msg, strlen(msg)+1, 0);
	rv = recv(sockfd, buf, MAXMSGLEN, 0);
	if (rv<0) err(1,0);
	buf[rv]=0;
	orig_close(sockfd);
	return 1;
}

//split the string with ''(space) to get separate argument
void split(char *src,const char *separator,char **dest,int cutnum) {
     char *pNext;
     int count;
     if (src == NULL || strlen(src) == 0)
        return;
     if (separator == NULL || strlen(separator) == 0)
        return;
     pNext = strtok(src,separator);
     *dest = pNext;
     count = 1;
     while(pNext != NULL && count < cutnum) {
        pNext = strtok(NULL,separator);
        dest[count]= pNext;
        ++count;
    }
}
