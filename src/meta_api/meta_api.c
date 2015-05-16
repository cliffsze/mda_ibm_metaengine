/* Filename : meta_api.c                                            */
/* Purpose  : Interface module allowing python code to access the   */
/*            Meta Engine API                                       */
/*                                                                  */
/* Change Log:                                                      */
/* 20150515 - initial release                                       */
/*                                                                  */
/* Compile command:                                                 */
/* cd /root/pyproj/meta_api                                         */    
/* gcc -fPIC -shared -I/usr/local/include/python2.7 -lpython2.7 \   */
/* -ometa_api.so meta_api.c libmetaengine.a ../hiredis/libhiredis.a */



#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../hiredis/hiredis.h"
#include "metaengine.h"



/* global variables */



redisContext *dbhandle;
int recid;



/* python def : rc = init_redis_handle( hostname ) */



static PyObject * pyInitRedisHandle(PyObject *self, PyObject *args) {

    char *hostname;

    if (!PyArg_ParseTuple(args, "s", &hostname)) {
        return Py_BuildValue("i", 1);   /* ERROR - PyArg_ParseTuple */
    }
    dbhandle = MEconnectRedis((char*)hostname, 6379);
    return Py_BuildValue("i", 0);       /* SUCCESS */
}



/* python def : rc = add_new_record( dict ) */



static PyObject * pyAddNewRecord(PyObject *self, PyObject *args) {

    int len, kvcount, kvstate, i, j;
    char key[1024], val[1024];
    char *dict;

    /* retrieve string in python dict format */
    if (!PyArg_ParseTuple(args, "s", &dict)) {
        return Py_BuildValue("i", 1);   /* ERROR - PyArg_ParseTuple */
    }

    /* string parsing format: {'four': 4, 'three': 3, 'five': 5} */
    len = strlen(dict);
    i = 0;         /* index into dict */
    kvcount = 0;
    kvstate = 0;   /* 0={, 1=key, 2=val, 3=end */
    
    for (i=0; i<len; i++) {
        if (kvstate == 0) {             /* kvstate=0, parse curly bracket */
            if (dict[i] == '{') {
                kvstate = 1;
                j = 0;
            }
        }
        else if (kvstate == 1) {        /* kvstate=1, parse for key */
            if (dict[i] == ':') {       /* delim = : */
                key[j] = 0x00;
                kvstate = 2;
                j = 0;
            }
            else if (j>0 || dict[i] != 0x20) {
                key[j] = dict[i];
                j++;
            }
        }
        else if (kvstate == 2) {        /* kvstate=2, parse for value */
            if (dict[i] == ',' || dict[i] == '}') {   /* delim = , or } */
                val[j] = 0x00;
                
                /* add key val pair into meta engine */
                kvcount++;
                printf("kv: %d %s %s\n", kvcount, key, val);
                if (kvcount == 1) {
                    recid = MEaddNewKeyValue(dbhandle, key, val);
                }
                else {
                    MEaddKeyValue(dbhandle, recid, key, val);
                }

                /* continue if comma, done if curly bracket */
                kvstate = 3;
                if (dict[i] == ',') {
                    kvstate = 1;
                    j = 0;
                }
            }
            else if (j>0 || dict[i] != 0x20) {
                val[j] = dict[i];
                j++;
            }
        }
    }
    if (kvstate == 3) {                 /* kvstate=3, end curly bracket found */
        return Py_BuildValue("i", 0);   /* SUCCESS */
    }
    else {
        return Py_BuildValue("i", 2);   /* ERROR - dict format error */
    }
}



/* bind python function names to c functions */



static PyMethodDef MetaApiMethods[] = {
    {"init_redis_handle", pyInitRedisHandle, METH_VARARGS},
    {"add_new_record", pyAddNewRecord, METH_VARARGS},
    {NULL, NULL}
};



/* python __init__ */



void initmeta_api()
{
    (void) Py_InitModule("meta_api", MetaApiMethods);
};
