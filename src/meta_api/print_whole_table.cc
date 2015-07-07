#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include "../hiredis/hiredis.h"
#include "metaengine.h"
#include <sys/stat.h>
#include <vector>
#include <map>
#include <string>

int main ( int argc, char *argv[] )
{
  redisContext *c;
  redisReply *reply = NULL;
  unsigned i,j, size=0;
  char *allData;

  if ( argc != 1 ) {
    printf( "usage: %s\n", argv[0] );
  } else {
      c = MEconnectRedis((char*)"localhost", 6379); 
      /* 
      c = MEconnectRedis((char*)"pub-redis-15728.dal-05.1.sl.garantiadata.com", 15728); 
      redisCommand(c,"AUTH GahBQ0HUTTNGowlE");
       */
      
      /*
       * Found out all keys
       */
      std::vector<std::string> allkeys;
      MEgetAllKeys(c, &reply);
      for ( i=0; i<reply->elements; ++i ) {
        // printf( "allkeys: %s\n", reply->element[i]->str );
        allkeys.push_back(reply->element[i]->str);
      }
      freeReplyObject(reply);

      printf("id");
      for( std::vector<std::string>::iterator it=allkeys.begin(); it!=allkeys.end(); it++){
        printf("\t%s ",it->c_str());
      }
      printf("\n");

      int lastId=MEgetLastId(c);

      for ( i=0; i<lastId; ++i ) {
        MEgetAllFieldsById(c, i, &allData, &size);
	if (size <= 0) {
	  if (allData) free(allData);
	  continue;
	}
	std::map<std::string,std::string,std::less<std::string> > aRow;
        for ( j=0; j<size; j+=2 ) {
	  // printf ("%u %u long.\n",(unsigned)strlen((allData+j*ME_STRING_MAX_LENGTH)), (unsigned)strlen((allData+(j+1)*ME_STRING_MAX_LENGTH)));
          aRow[(allData+j*ME_STRING_MAX_LENGTH)]=(allData+(j+1)*ME_STRING_MAX_LENGTH);
          // printf( "all field: %s %s\n", (allData+j*ME_STRING_MAX_LENGTH),(allData+(j+1)*ME_STRING_MAX_LENGTH) );
        }
        free(allData);
        printf("%d",i);
        for( std::vector<std::string>::iterator it=allkeys.begin(); it!=allkeys.end(); it++){
          printf("\t%s ",aRow[*it].c_str());
        }
        printf("\n");
      }
      redisFree(c);
  }
  return 0;
}

