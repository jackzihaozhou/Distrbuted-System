/*
 *  Name: Zihao ZHOU ID:zihaozho
 */

/*
 *  a server of simple rpc implementation
 */

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include<errno.h>
#define MAXMSGLEN 1000100
#define MAXSTORE 1000100
#define MAXSTATE 800
#define MAXPARA 8

void split(char *src,const char *separator,char **dest,int cutnum);
void select_action(char *para,int sessfd);
void do_open1(char **para,int sessfd);
void do_open2(char **para,int sessfd);
void do_close(int closefd,int sessfd);
void do_read(int sessfd,int fd, int count);
void do_write(int sessfd,int fd, int count,char * buf);
void do_lseek(int sessfd, int fd, off_t offset, int whence);
void do_xstat(char ** dest,int sessfd);
void do_getdirentries(int sessfd,int fd,int nbytes,off_t basep);
void do_unlink(int sessfd,char *path);

int offset_in(int fd);
int offset_out(int fd);

//set up network and receive message from client permanently
int main(int argc, char**argv) {
	char *msg="Hello from server";
	char buf[MAXMSGLEN+1]={0};
	char emptybuf[MAXMSGLEN+1]={0};
	char length_buf[9]={0};
	char *serverport;
	unsigned short port;
	int sockfd, sessfd, rv, i;
	struct sockaddr_in srv, cli;
	socklen_t sa_size;
	int opcode;
	int argnum = 0;
	// Get environment variable indicating the port of the server
	serverport = getenv("serverport15440");
	if (serverport) port = (unsigned short)atoi(serverport);
	else port=15440;

	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd<0) err(1, 0);

	// setup address structure to indicate server port
	memset(&srv, 0, sizeof(srv));
	srv.sin_family = AF_INET;
	srv.sin_addr.s_addr = htonl(INADDR_ANY);
	srv.sin_port = htons(port);
	// bind to our port
	rv = bind(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);
	// start listening for connections
	rv = listen(sockfd, 5);
	if (rv<0) err(1,0);

	int length = MAXMSGLEN;
    int rv_total = 0;
    char storebuf[MAXSTORE]={0};
	// main server loop, handle clients one at a time
	while(1){
		sa_size = sizeof(struct sockaddr_in);
		sessfd = accept(sockfd, (struct sockaddr *)&cli, &sa_size);
		//fork to deal with multi-client connection
		rv = fork();
        if (rv == 0) {
            // child process to deal with one client
            close(sockfd);
            while(1){//loop to make sure receive full-length message from client
                rv_total = 0;
                length = MAXMSGLEN;
                memcpy(buf,emptybuf,MAXMSGLEN+1);
                while (1) {
                    rv = recv(sessfd, &buf[rv_total], MAXMSGLEN, 0);
                    memcpy(length_buf,buf, 8);
                    length = atoi(length_buf);
                    if(rv<=0){
                        //after finish operation. connect break
                        exit(0);
                        break;
                    }
                    rv_total = rv_total + rv;
                    if(rv_total>=length){
                        break;
                    }
                }
                //act operation from client
                select_action(&buf[8],sessfd);
            }
        }
        // parent process
		if (sessfd<0) err(1,0);
		// either client closed connection, or error
		if (rv<0) err(1,0);
		close(sessfd);
	}
	close(sockfd);
	return 0;
}
//grab the opcode from message and select action to operate
void select_action(char *buf, int sessfd){

    char *dest[MAXPARA]={0};
    int opcode = 0;
    int fd,count,nbytes = 0;
    off_t offset;
    int whence;
    int int_stat_buf;
    struct stat * stat_buf;
    off_t basep;
    char op_buf[9]={0};
    char fd_buf[9]={0};
    char count_buf[9]={0};
    char whence_buf[9]={0};
    char path[MAXSTORE]={0};
    memcpy(op_buf,buf,8);
    opcode = atoi(op_buf);

    if (opcode == 1){//open
        split(&buf[8]," ",dest,2);
        do_open1(dest,sessfd);
    }
    else if (opcode == 2){//open with mode
        split(&buf[8]," ",dest,3);
        do_open2(dest,sessfd);
    }
    else if (opcode == 3){//read
        memcpy(fd_buf,&buf[8],8);
        fd = atoi(fd_buf);
        fd = offset_in(fd);
        memcpy(count_buf,&buf[16],8);
        count = atoi(count_buf);
        do_read(sessfd,fd,count);

    }
    else if (opcode == 4){//close
        memcpy(fd_buf,&buf[8],8);
        fd = atoi(fd_buf);
        fd = offset_in(fd);
        do_close(fd,sessfd);
    }
    else if (opcode == 5){//write
        memcpy(fd_buf,&buf[8],8);
        fd = atoi(fd_buf);
        fd = offset_in(fd);
        memcpy(count_buf,&buf[16],8);
        count = atoi(count_buf);
        do_write(sessfd,fd,count,&buf[24]);
    }
    else if (opcode == 6){//lseek
        memcpy(fd_buf,&buf[8],8);
        fd = atoi(fd_buf);
        fd = offset_in(fd);
        memcpy(count_buf,&buf[16],8);
        offset = atoi(count_buf);
        memcpy(whence_buf,&buf[24],8);
        whence = atoi(whence_buf);
        do_lseek(sessfd,fd,offset,whence);
    }
    else if (opcode == 7){//stat
        split(&buf[8]," ",dest,2);
        do_xstat(dest,sessfd);
    }
    else if (opcode == 8){//getdirentries
        memcpy(fd_buf,&buf[8],8);
        fd = atoi(fd_buf);
        fd = offset_in(fd);
        memcpy(count_buf,&buf[16],8);
        nbytes = atoi(count_buf);
        memcpy(whence_buf,&buf[24],8);
        basep = atoi(whence_buf);
        do_getdirentries(sessfd,fd,nbytes,basep);
    }
    else if (opcode == 9){//unlink
        strcpy(path,&buf[8]);
        do_unlink(sessfd,path);
    }
}
//open operation
void do_open1(char **para,int sessfd){
    int sopenfd;
    char *pathname;
    int flags;
    char buf[MAXMSGLEN+1]={0};
    int length;
    ssize_t retssize = 0;
    pathname = para[0];
    flags = atoi(para[1]);
    sopenfd = open(pathname, flags);
    length = 24;

    if(sopenfd<=0){
        sprintf(buf, "%8d%8d%8d",length,errno,sopenfd);
        send(sessfd, buf, (size_t)length, 0);
    }
    else{
        sopenfd = offset_out(sopenfd);
        sprintf(buf, "%8d%8d%8d",length,errno,sopenfd);
        send(sessfd, buf, (size_t)length, 0);
    }
    fprintf(stderr, "do open1 errno = %d\n", errno);
    fprintf(stderr, "do open1 sopenfd = %d\n", sopenfd);
}
//open operation with mode
void do_open2(char **para,int sessfd){
    int sopenfd;
    char *pathname;
    int flags,m;
    int length;
    char buf[MAXMSGLEN+1]={0};
    pathname = para[0];
    flags = atoi(para[1]);
    m = atoi(para[2]);

    sopenfd = open(pathname, flags,m);
    length = 24;
    if(sopenfd<=0){
        sprintf(buf, "%8d%8d%8d",length,errno,sopenfd);
        send(sessfd, buf, (size_t)length, 0);
    }
    else{
        sopenfd = offset_out(sopenfd);
        sprintf(buf, "%8d%8d%8d",length,errno,sopenfd);
        send(sessfd, buf, (size_t)length, 0);
    }
    fprintf(stderr, "do open2 sopenfd = %d\n", sopenfd);
    fprintf(stderr, "do open2 errno = %d\n", errno);
}
//read operation
void do_read(int sessfd,int readfd, int count){
    char buf[MAXSTORE+1]={0};
    char bufwaste[MAXSTORE+1]={0};
    char bufcontent[MAXSTORE+1]={0};
    ssize_t retssize = 0;

    retssize = read(readfd, bufcontent, (unsigned long)count);

    int length = 24 + retssize;
    sprintf(buf,"%8d%8d%8ld", length,errno,retssize);
    memcpy(&buf[24],bufcontent, (size_t)retssize);
    send(sessfd, buf, (size_t)length, 0);
}
//write operation
void do_write(int sessfd,int writefd, int count,char * buf){
    ssize_t retssize = 0;
    char sendbackbuf[MAXSTORE+1]={0};
	int rv = 0;
    int length = 24;
    retssize =  write(writefd,buf,(size_t)count);
    fprintf(stderr, "do write errno = %d\n", errno);
    sprintf(sendbackbuf,"%8d%8d%8ld",length,errno,retssize);
    send(sessfd, sendbackbuf, length, 0);
    fprintf(stderr, "do write retssize = %ld\n", retssize);
    fprintf(stderr, "do write errno = %d\n", errno);
}
//lseek operation
void do_lseek(int sessfd, int fd, off_t offset, int whence){
    char storebuf[MAXSTORE+1]={0};
    char sendbackbuf[MAXSTORE+1]={0};
    int rv = 0;
    int length = 24;
    off_t lseekret;
    lseekret = lseek(fd, offset, whence);
    sprintf(sendbackbuf,"%8d%8d%8ld",length,errno,lseekret);
    rv = send(sessfd, sendbackbuf, strlen(sendbackbuf), 0);
    fprintf(stderr, "do lseek lseekret = %ld\n", lseekret);
    fprintf(stderr, "do lseek errno = %d\n", errno);

}
//close operation
void do_close(int closefd,int sessfd){
    char buf[MAXMSGLEN+1]={0};
    int closeret = -1;
    int length = 24;
    closeret = close(closefd);
    sprintf(buf,"%8d%8d%8d",length,errno,closeret);
    send(sessfd, buf, (size_t)length, 0);
    fprintf(stderr, "do close closeret = %d\n", closeret);
    fprintf(stderr, "do close errno = %d\n", errno);
}
void do_xstat(char **para,int sessfd){
    int sopenfd;
    char *path;
    int ver;
    int xstatret;
    int length;
    char buf[MAXMSGLEN+1]={0};
    char temp[MAXSTORE+1]={0};
    ssize_t retssize = 0;
    path = para[0];
    ver = atoi(para[1]);
    struct stat *stat_buf;
    stat_buf = (struct stat *)malloc(MAXSTATE-24);
    xstatret = __xstat(ver,path,stat_buf);
    //we don't know the exact length of the reading stat from stat_buf so set the length as MAXLENGTH
    length = MAXSTATE;
    sprintf(buf, "%8d%8d%8d" ,length,errno,xstatret);
    memcpy(&buf[24],stat_buf, (size_t)(MAXSTATE-24));
    send(sessfd, buf, (size_t)length, 0);
    fprintf(stderr, "do xstat errno = %d\n", errno);
    fprintf(stderr, "do xstat xstatret = %d\n", xstatret);
    free(stat_buf);
}
//getdirentries operation
void do_getdirentries(int sessfd,int fd,int nbytes,off_t basep){
    ssize_t ret;
    int length;
    char storebuf[MAXMSGLEN+1]={0};
    char buf[MAXMSGLEN+1]={0};
    ret = getdirentries(fd, storebuf, (size_t)nbytes, &basep);
    if(ret>0){
        length = 32+ret;
        sprintf(buf,"%8d%8d%8ld%8ld",length,errno,ret,basep);
        memcpy(&buf[32],storebuf, (size_t)ret);
        send(sessfd, buf, (size_t)length, 0);
    }
    else{
        length = 32;
        sprintf(buf,"%8d%8d%8ld%8ld",length,errno,ret,basep);
        send(sessfd, buf, (size_t)length, 0);
    }
    fprintf(stderr, "do_getdirentries ret = %ld\n", ret);
    fprintf(stderr, "do_getdirentries errno = %d\n", errno);
}
//unlink operation
void do_unlink(int sessfd,char *path){
     int ret;
     ret = unlink(path);
     int length = 24;
     char buf[MAXMSGLEN+1]={0};
     sprintf(buf,"%8d%8d%8d",length,errno,ret);
     send(sessfd, buf, (size_t)length, 0);
     fprintf(stderr, "do_unlink errno = %d\n", errno);
     fprintf(stderr, "do_unlink ret = %d\n", ret);

}
//split the string with ''(space) to get string pathname
void split(char *src,const char *separator,char **dest,int cutnum){
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
//offset operation to distinguish the fd between server and client
int offset_out(int fd){
    return(fd+5000);
}
int offset_in(int fd){
    return(fd-5000);
}
