#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include "leveldb/status.h"
//using namespace std;

namespace leveldb {
 
class HashFunc {
public:
    unsigned int RSHash( char *str,int length)
{
 unsigned int b = 378551;
	unsigned int a = 63689;
	unsigned int hash = 0;
	int i=0;
	while(i<length){
		hash = hash * a + (*str ++);
		a = a * b;
		i++;
	}
	
	return (hash & 0x7FFFFFFF);
}

unsigned int JSHash( char *str, int length)
{
	 unsigned int hash = 1315423911;
	 int i=0;
	 while(i<length){
	 //while(*str){
			hash ^= ((hash << 5) + (*str++) + (hash >> 2));
			i++;
	 }
	 return (hash & 0x7FFFFFFF);
}

unsigned int PJWHash( char *str, int length) 
{
  const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);    
  const unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);    
  const unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);    
  const unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);    
  unsigned int hash              = 0;    
  unsigned int test              = 0;
	int i=0;
	while(i<length){
	//while(*str){
		hash = (hash << OneEighth) + (*str ++);    
	  if((test = hash & HighBits)  != 0){
			hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
		}
		i++;
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int ELFHash( char *str)
{
	unsigned int hash = 0;    
	unsigned int x    = 0;
	while(*str){
		hash = (hash << 4) + (*str++);
		if((x = hash & 0xF0000000L) != 0){    
			hash ^= (x >> 24);    
		}    
	  hash &= ~x;
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int BKDRHash( char *str)
{
	unsigned int seed = 131; /* 31 131 1313 13131 131313 etc.. */    
  unsigned int hash = 0;
  while(*str){
		hash = (hash * seed) + (*str++);
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int SDBMHash( char *str, int length)
{
	unsigned int hash = 0;
	int i=0;
	while(i<length){
	//while(*str){
		hash = (*str ++) + (hash << 6) + (hash << 16) - hash;
		i++;
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int DJBHash( char *str, int length)
{
	unsigned int hash = 5381;
	int i=0;
	while(i<length){
	//while(*str){
		hash = ((hash << 5) + hash) + (*str++);
		i++;
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int DEKHash( char *str, int length)
{
	unsigned int hash = strlen(str);
//	cout<< " in DEKHash for test let me see the strlen is "<< hash <<endl;
	int i=0;
	while(i<length){
	//while(*str){
		hash = ((hash << 5) ^ (hash >> 27)) ^ (*str ++);
		i++;
	}
	return (hash & 0x7FFFFFFF);
}
unsigned int BPHash( char *str, int length)
{
	unsigned int hash = strlen(str);
	int i=0;
	while(i<length){
	//while(*str){
		hash = hash << 7 ^ (*str ++);
		i++;
	}
	return (hash & 0x7FFFFFFF);
}
unsigned int FNVHash( char *str,int length)
{
	const unsigned int fnv_prime = 0x811C9DC5;    
	unsigned int hash      = 0;
	int i=0;
	while(i<length){
	//while(*str){
		hash *= fnv_prime;    
    hash ^= (*str ++);
		i++;
	}
	return (hash & 0x7FFFFFFF);
}
unsigned int APHash( char *str, int length)
{
	unsigned int hash = 0xAAAAAAAA;
	//int len = strlen(str);
	unsigned int i = 0;
	for(i = 0; i < length; str++, i++){
		hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ (*str) * (hash >> 3)) :    
		                         (~((hash << 11) + (*str) ^ (hash >> 5)));
	}
	return (hash & 0x7FFFFFFF);
}

unsigned int UniHash( char *str)
{   unsigned int InKey;
	 if(strlen(str)>20){
		 	//char substr[config::kKeyLength];
		//	 memset(substr,0,config::kKeyLength);
		//	strncpy(substr, str+4, config::kKeyLength);
			InKey=strtoul(str+4,NULL,10);
      //InKey=strtoul(str.substr(4,config::kKeyLength).c_str(),NULL,10);
	 }else{
      InKey=strtoul(str,NULL,10);
   }
	 return InKey;
}

unsigned int cuckooHash(char *substr,int which, int length){
	switch(which){
		case 0:
			//return UniHash(substr);
			return FNVHash(substr,length)%config::bucketNum;
		case 1:
			return JSHash(substr,length)%config::bucketNum;
		case 2:
			return PJWHash(substr,length)%config::bucketNum;
		case 3:
			return APHash(substr,length)%config::bucketNum;
		case 4:
			return BPHash(substr,length)%config::bucketNum;
		case 5:
			return DJBHash(substr,length)%config::bucketNum;
		case 6:
			return SDBMHash(substr,length)%config::bucketNum;
		case 7:
			return DEKHash(substr,length)%config::bucketNum;
		default:
			return UniHash(substr);
	}
}

unsigned int HashValue( char *str,int index,unsigned int SIZE )
{
	switch(index)
	{
		case 0:
			return FNVHash(str,strlen(str))%(SIZE*8);	
		case 1:
			return JSHash(str,strlen(str))%(SIZE*8);
		case 2:
			return BPHash(str,strlen(str))%(SIZE*8);
		case 3:
			return DJBHash(str,strlen(str))%(SIZE*8);
		case 4:
			return BKDRHash(str)%(SIZE*8);
		case 5:
			return SDBMHash(str,strlen(str))%(SIZE*8);
		case 6:
			return APHash(str,strlen(str))%(SIZE*8);
		case 7:
			return DEKHash(str,strlen(str))%(SIZE*8);
		case 8:
			return PJWHash(str,strlen(str))%(SIZE*8);
		case 9:
			return RSHash(str,strlen(str))%(SIZE*8);
		//case 10:
		//	return ELFHash(str)%(SIZE*8);
		default:
			return 0;
	}
}

bool GenerateHashValue( char *str,unsigned int *Ret,int hashNum,unsigned int size)
{
	if(hashNum<=0){
		printf("in hash funs GenerateHashValue,hashnum less than zero");
		return false;
	}
	int i;
	for(i = 0; i <hashNum; i++){
		Ret[i] =HashValue(str,i,size); // get every hashvalue
	}
	return true;
}
};
  
  
}
