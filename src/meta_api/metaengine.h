#ifndef _METAENGINE_H
#define _METAENGINE_H

#define ME_STRING_MAX_LENGTH 1024

redisContext *MEconnectRedis(const char *hostname, const int port);
int MEaddNewKeyValue(redisContext *c, const char *key, const char *value);
int MEaddKeyValue(redisContext *c, int id, const char *key, const char *value);
int MEaddKeyValueToAllMatched(redisContext *c, const char *matchKey, const char *matchValue, const char *newKey, const char *newValue);
int MEcreateNewRecordWithKeyValue(redisContext *c, const char *key, const char *value);
int MEgetAllKeys(redisContext *c, redisReply **reply);
int MEsearch(redisContext *c, const char *key, const char *pattern, char **allData, unsigned *size);
int MEsearchId(redisContext *c, const char *key, const char *pattern, int **allId, unsigned *size);
int MEgetAllFieldsById(redisContext *c, const int id, char **allData, unsigned *size);
int MEgetSingleFieldById(redisContext *c, const int id, const char* key, char **allData);

#endif
