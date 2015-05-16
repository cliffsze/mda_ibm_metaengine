#ifndef _METAENGINE_H
#define _METAENGINE_H

#define ME_STRING_MAX_LENGTH 1024

redisContext *MEconnectRedis(const char *hostname, const int port);
int MEaddNewKeyValue(redisContext *c, const char *key, const char *value);
int MEaddKeyValue(redisContext *c, int id, const char *key, const char *value);
int MEaddKeyValueToAllMatched(redisContext *c, const char *matchKey, const char *matchValue, const char *newKey, const char *newValue);
int MEcreateNewRecordWithKeyValue(redisContext *c, const char *key, const char *value);
int MEgetAllKeys(redisContext *c, redisReply **reply);
int MEsearch(redisContext *c, const char *key, const char *pattern, char allData[][ME_STRING_MAX_LENGTH], size_t *size);

#endif
