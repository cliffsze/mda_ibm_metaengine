// Filename : meta_api.c
// Purpose  : Interface module allowing python code to access the Meta Engine API
//
// Change Log:
// 20150515 - initial release
// 20150625 - rewritten pyAddNewRecord
// 20150625 - added pyAppendToRecord, pyGetUnprocessedRecord
// 21050628 - added debugMode to manage printf to stdout
//
// Compile command:
// cd /root/pyproj/meta_api    
// gcc -fPIC -shared -I/usr/local/include/python2.7 -lpython2.7 \ 
// -ometa_api.so meta_api.c libmetaengine.a ../hiredis/libhiredis.a



#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "/root/pyproj/hiredis/hiredis.h"
#include "metaengine.h"


// global variables
redisContext *dbhandle;
int *debugMode;    // 1=true, 0=false


// python def : rc = init_redis_handle( hostname, debug_mode )
// rc: 0=success, 1=input format error


static PyObject * pyInitRedisHandle(PyObject *self, PyObject *args) {

    char *hostname;
    int* debug;

    if (!PyArg_ParseTuple(args, "si", &hostname, &debug)) {
        return Py_BuildValue("i", 1);   /* ERROR - PyArg_ParseTuple */
    }
    dbhandle = MEconnectRedis((char*)hostname, 6379);
    debugMode = debug;
    if (debugMode) {
        printf("pyInitRedisHandle: debugMode=%i\n", debugMode);
    }

    return Py_BuildValue("i", 0);       /* SUCCESS */
}



// python def : rc = add_new_record( dict )
// loop through input dict object and add each kv-pair to the meta engine as one record
// rc: 0=success, 1=input format error, 2=input format error - dict iterator



static PyObject * pyAddNewRecord(PyObject *self, PyObject *args) {

    PyObject* py_dict;
    PyObject* py_iter;
    PyObject* py_key;
    PyObject* py_val;

    // retrieve dictionary object
    if (!PyArg_ParseTuple(args, "O!", &PyDict_Type, &py_dict)) {
        return Py_BuildValue("i", 1);   // ERROR - PyArg_ParseTuple
    }

    // retrieve iterator
    py_iter = PyObject_GetIter(py_dict);
    if (!py_iter) { 
        return Py_BuildValue("i", 2);   // ERROR - Not an iterator
    }

    // loop through key-value pairs
    int recid;
    int kvcount = 0;
    while (py_key = PyIter_Next(py_iter)) {
        py_val = PyDict_GetItem(py_dict, py_key);

    char * key = PyString_AsString(py_key);
    char * val = PyString_AsString(py_val);
    if (debugMode) {
        printf("pyAddNewRecord: %s - %s\n", key, val);
    }

    // add kv pair into meta engine
    kvcount++;
    if (kvcount == 1) {
        recid = MEaddNewKeyValue(dbhandle, key, val);
    }
    else {
        MEaddKeyValue(dbhandle, recid, key, val);
    }
    Py_DECREF(py_key);
}
Py_DECREF(py_iter);
if (debugMode) {
    printf("pyAddNewRecord: total kv count: %i\n", kvcount);
}
return Py_BuildValue("i", 0);   // SUCCESS
} 



// python def : rc = append_to_record( recid, dict )
// Append new fields into an existing record. New fields are stored in the dict object as KV pairs
// rc: 0=success, 1=input format error, 2=recid not found, 3=input format error - dict iterator



static PyObject * pyAppendToRecord(PyObject *self, PyObject *args) {

    PyObject* py_dict;
    PyObject* py_iter;
    PyObject* py_key;
    PyObject* py_val;

    int recid, rc;
    char *allData;
    unsigned size;


    // retrieve record id and dictionary object
    if (!PyArg_ParseTuple(args, "iO!", &recid, &PyDict_Type, &py_dict)) {
        return Py_BuildValue("i", 1);   // ERROR - PyArg_ParseTuple
    }

    // abort if record id does not exist
    rc = MEgetAllFieldsById(dbhandle, recid, &allData, &size);
    if (size == 0) {
        return Py_BuildValue("i", 2);   // ERROR - record ID not found   
    }

    // retrieve iterator
    py_iter = PyObject_GetIter(py_dict);
    if (!py_iter) { 
        return Py_BuildValue("i", 3);   // ERROR - Not an iterator
    }

    // loop through key-value pairs and append to record
    int kvcount = 0;
    while (py_key = PyIter_Next(py_iter)) {
        py_val = PyDict_GetItem(py_dict, py_key);

    char * key = PyString_AsString(py_key);
    char * val = PyString_AsString(py_val);
    if (debugMode) {
        printf("pyAppendToRecord: %s - %s\n", key, val);
    }

    // add kv pair into meta engine
    kvcount++;
    MEaddKeyValue(dbhandle, recid, key, val);
    Py_DECREF(py_key);
}
Py_DECREF(py_iter);
if (debugMode) {
    printf("pyAppendToRecord: total kv count: %i\n", kvcount);
}
return Py_BuildValue("i", 0);   // SUCCESS
} 



// python def : (rc, dict) = get_unprocessed_record( search_key, search_value, except_key )
// find records matches the search_key and not the except_key, return list of items
// rc: 0=success, 1=input format error, 2=search_key - no match found
// dict: (recid: recid[searchKey] ...)



static PyObject * pyGetUnprocessedRecord(PyObject *self, PyObject *args) {

    // retrieve 3 input parameters in char * format
    char *searchKey, *searchVal, *exceptKey;
    if (!PyArg_ParseTuple(args, "sss", &searchKey, &searchVal, &exceptKey)) {
        return Py_BuildValue("i", 1);   // ERROR - PyArg_ParseTuple
    }

    // get all recID that matches the searckKey and searchVal
    int *allId;    
    unsigned size = 0;
    unsigned unprocessed_record_count = 0;
    MEsearchId(dbhandle, searchKey, searchVal, &allId, &size);
    if (size == 0) {
        return Py_BuildValue("i", 2);   // ERROR - no record found
    }

    // build output dict object
    int rc, i;
    long recid;
    char *value=NULL;
    PyObject* py_dict;
    PyObject* py_key;
    PyObject* py_val;
    py_dict = PyDict_New();

    // loop through all recIDs
    for ( i=0; i<size; ++i ) {
        recid = *(allId+i);
        rc = MEgetSingleFieldById(dbhandle, recid, exceptKey, &value);

        // exceptKey not found, create kv and add to py_dict (recid: recid[searchKey])
        if (rc > 0 || value == 0) {
            unprocessed_record_count += 1;
            rc = MEgetSingleFieldById(dbhandle, recid, searchKey, &value);
            py_key = PyInt_FromLong(recid);
            py_val = PyString_FromString(value);
            PyDict_SetItem(py_dict, py_key, py_val);
        }
        free(value);
    }
    free(allId);
    
    if (debugMode) {
        printf("pyGetUnprocessedRecord: total records: %i\n", size);
        printf("pyGetUnprocessedRecord: unprocessed records: %i\n", unprocessed_record_count);
    }

    // return 2 parameters: rc, dict
    return Py_BuildValue("iO", 0, py_dict);
}



// python def : (rc, dict) = get_records( search_key, search_value )
// find records matches the search_key and return list of items
// rc: 0=success, 1=input format error, 2=search_key - no match found
// dict: (recid: recid[searchKey] ...)



static PyObject * pyGetRecords(PyObject *self, PyObject *args) {

    // retrieve 2 input parameters in char * format
    char *searchKey, *searchVal;
    if (!PyArg_ParseTuple(args, "ss", &searchKey, &searchVal)) {
        return Py_BuildValue("i", 1);   // ERROR - PyArg_ParseTuple
    }

    // get all recID that matches the searckKey and searchVal
    int *allId;
    unsigned recCount;
    MEsearchId(dbhandle, searchKey, searchVal, &allId, &recCount);
    if (recCount == 0) {
        return Py_BuildValue("i", 2);   // ERROR - no record found
    }

    // build output dict object
    int rc, i, j;
    unsigned size;
    long recid, dict_size;
    char *allData, *key, *val;
    PyObject* py_rdict;
    PyObject* py_adict;
    PyObject* py_key;
    PyObject* py_val;
    py_rdict = PyDict_New();
    py_adict = PyDict_New();

    // loop through all recIDs
    for ( i=0; i<recCount; ++i ) {
        recid = *(allId+i);
        PyDict_Clear(py_adict);

        // create kv and add to py_dict (recid: recid[searchKey])
        MEgetAllFieldsById(dbhandle, recid, &allData, &size);
        for (j=0; j<size; j+=2) {
            key = allData+j*ME_STRING_MAX_LENGTH;
            val = allData+(j+1)*ME_STRING_MAX_LENGTH;
            if (debugMode) {
                printf( "%i: %s %s\n", recid, key, val);
            }
            py_key = PyString_FromString(key);
            py_val = PyString_FromString(val);
            PyDict_SetItem(py_adict, py_key, py_val);
        }
        py_key = PyInt_FromLong(recid);
        PyDict_SetItem(py_rdict, py_key, PyDict_Copy(py_adict));
        free(allData);
    }
    free(allId);
    
    if (debugMode) {
        printf("pyGetRecords: records retrieved: %i\n", recCount);
    }

    // return 2 parameters: rc, dict
    return Py_BuildValue("iO", 0, py_rdict);
}



/* bind python function names to c functions */



static PyMethodDef MetaApiMethods[] = {
    {"init_redis_handle", pyInitRedisHandle, METH_VARARGS},
    {"add_new_record", pyAddNewRecord, METH_VARARGS},
    {"append_to_record", pyAppendToRecord, METH_VARARGS},
    {"get_unprocessed_record", pyGetUnprocessedRecord, METH_VARARGS},
    {"get_records", pyGetRecords, METH_VARARGS},
    {NULL, NULL}
};



/* python __init__ */



void initmeta_api()
{
    (void) Py_InitModule("meta_api", MetaApiMethods);
};
