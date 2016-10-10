/*
MIT License

	Copyright (c) 2016 Joe Oszlanczi rawengroup@gmail.com

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

extern "C"
{

#include <lua.h>
#include <lauxlib.h>

#include "dbapi.h"
#include "indexapi.h" 

};

#include "lwhitedb.h"
#include <string.h>
#include <assert.h>

#define  WHITEDB_METATABLE          ":whitedb_meta_table:"
#define  WHITEDB_RECORD_METATABLE   ":whitedb_record_meta_table:"
#define  WHITEDB_NAME               "whitedb"
#define  WHITEDB_MAX_FIND_STR_SIZE  64

#define DWhiteDbNameSize 64
#define DWhiteDbMaxMultiIndexSize 16
#define DWhiteDbMaxQuerySize 20

#define DWhiteDbVersion "0.1.1"

//static int whitedb_record_metatable_ref;

//---------------------------------------------------------
// iterator modes
// 1 - normal database record iterator
// 2 - normal database parent iterator

typedef struct whitedb_record_iterator
{
	void* pWhiteDb;
	void* pRecord;
	void* pParent;
	int   iMode;
	int   whitedb_record_metatable_ref;
} whitedb_record_iterator;

//---------------------------------------------------------
typedef struct whitedb_find_iterator
{
	void* pWhiteDb;
	void* pRecord;
	int   iFieldIndex;
	int   iFieldType;
	int   iFiendCond;
	double dValue;
	int    bValue;
	char   sValue[WHITEDB_MAX_FIND_STR_SIZE + 1];
	int   whitedb_record_metatable_ref;
} whitedb_find_iterator;


//---------------------------------------------------------
typedef struct whitedb_instance
{
	void* pWhiteDb;
	int   iMode;
	int   iPermissions;
	char  sName[DWhiteDbNameSize + 1];
	wg_int iLockReadTr;
	wg_int iLockWriteTr;
	int    iLockRead;
	int    iLockWrite;
	int    whitedb_record_metatable_ref;
} whitedb_instance;

//---------------------------------------------------------
typedef struct whitedb_record
{
	void* pWhiteDb;
	void* pRecord;
	wg_int iError;
	int    whitedb_record_metatable_ref;
} whitedb_record;

//---------------------------------------------------------
typedef struct whitedb_query_iterator
{
	void*     pWhiteDb;
	wg_query* pQuery;
	int    whitedb_record_metatable_ref;
} whitedb_query_iterator;

//---------------------------------------------------------
static whitedb_instance* check_instance(lua_State *l, int iIndex)
{
	assert(lua_gettop(l) > 0);
#ifdef _DEBUG
	whitedb_instance* instance = (whitedb_instance*)luaL_checkudata(l, iIndex, WHITEDB_METATABLE);
#else
	whitedb_instance* instance = (whitedb_instance*)lua_touserdata(l, iIndex);
#endif
	assert(instance);
	return instance;
}
//---------------------------------------------------------
#define INSTANCE_EXIT_BOOL(NAME) if ( !NAME ) { lua_pushboolean(l, 0 ); return 1; }
#define INSTANCE_EXIT_NIL(NAME) if ( !NAME ) { lua_pushnil(l); return 1; }

//---------------------------------------------------------
static const char* STR_COND_EQUAL = "=";
static const char* STR_COND_NOT_EQUAL = "!=";
static const char* STR_COND_LESSTHAN = "<";
static const char* STR_COND_GREATER = ">";
static const char* STR_COND_LTEQUAL = "<=";
static const char* STR_COND_GTEQUAL = ">=";

//---------------------------------------------------------
const char* cond_to_str(int iCondition)
{
	switch (iCondition)
	{
		case WG_COND_EQUAL: return STR_COND_EQUAL;
		case WG_COND_NOT_EQUAL: return STR_COND_NOT_EQUAL;
		case WG_COND_LESSTHAN: return STR_COND_LESSTHAN;
		case WG_COND_GREATER: return STR_COND_GREATER;
		case WG_COND_LTEQUAL: return STR_COND_LTEQUAL;
		case WG_COND_GTEQUAL: return STR_COND_GTEQUAL;
		default:
			assert(0);
			break;
	}
	return NULL;
}

//---------------------------------------------------------
int str_to_cond(const char* sCondition)
{
	if ( strcmp(sCondition, STR_COND_EQUAL) == 0)
		return WG_COND_EQUAL;

	if (strcmp(sCondition, STR_COND_NOT_EQUAL) == 0)
		return WG_COND_NOT_EQUAL;

	if (strcmp(sCondition, STR_COND_LESSTHAN) == 0)
		return WG_COND_LESSTHAN;

	if (strcmp(sCondition, STR_COND_GREATER) == 0)
		return WG_COND_GREATER;

	if (strcmp(sCondition, STR_COND_LTEQUAL) == 0)
		return WG_COND_LTEQUAL;

	if (strcmp(sCondition, STR_COND_GTEQUAL) == 0)
		return WG_COND_GTEQUAL;
	
	assert(0);
	return 0;
}

//---------------------------------------------------------
static int whitedb_record_to_userdata(int whitedb_record_metatable_ref, void* pWhiteDb, void* pRecord, int iSize,  lua_State *l )
{
	assert(pWhiteDb);
	void* pIntRecord = pRecord == NULL ? wg_create_record(pWhiteDb, iSize) : pRecord;
	if ( pIntRecord == NULL)
	{
		lua_pushnil(l);
		return 1;
	}

	whitedb_record* pNewIntRecord = (whitedb_record*)lua_newuserdata(l, sizeof(whitedb_record));
	pNewIntRecord->pRecord  = pIntRecord;
	pNewIntRecord->pWhiteDb = pWhiteDb;
	pNewIntRecord->whitedb_record_metatable_ref = whitedb_record_metatable_ref;
	lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_record_metatable_ref );
	lua_setmetatable(l, -2);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_createxx(lua_State *l)
{
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	assert(pRecord);
	int iSize = 1;
	if (lua_gettop(l) > 1)
		iSize = lua_tointeger(l, 2);
	return whitedb_record_to_userdata(pRecord->whitedb_record_metatable_ref, pRecord->pWhiteDb, NULL, iSize, l);
}

//---------------------------------------------------------
static int whitedb_record_create(lua_State *l)
{
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = (whitedb_instance*)lua_touserdata(l, 1);
	int               iSize = 1;
	if ( lua_gettop(l) > 1 )
		iSize = lua_tointeger(l, 2);

	assert(pInstance);
	assert(pInstance->pWhiteDb);
	return whitedb_record_to_userdata(pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, NULL, iSize, l);
}

//---------------------------------------------------------
static int whitedb_record_delete(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	assert(pRecord);
	wg_int iSuccess = wg_delete_record(pRecord->pWhiteDb, pRecord->pRecord);
	lua_pushboolean( l, iSuccess == 0);
	lua_pushinteger( l, iSuccess);
	return 2;
}

//---------------------------------------------------------
static int whitedb_records_iterator(lua_State *l)
{		
	whitedb_record_iterator* pIterator = (whitedb_record_iterator*)lua_touserdata(l, lua_upvalueindex(1));
	void*  pNext = NULL;

	if (pIterator->iMode == 1)
	{
		if (pIterator->pRecord)
			pNext = wg_get_next_record(pIterator->pWhiteDb, pIterator->pRecord);
		else
			pNext = wg_get_first_record(pIterator->pWhiteDb);
	}
	else if (pIterator->iMode == 2)
	{
		if (pIterator->pParent)
			pNext = wg_get_next_parent(pIterator->pWhiteDb, pIterator->pRecord, pIterator->pParent);
		else
			pNext = wg_get_first_parent(pIterator->pWhiteDb, pIterator->pRecord );
	}
	else
	{
		assert(0);
	}


	if (pNext)
	{
		if (pIterator->iMode == 1)
		{
			pIterator->pRecord = pNext;
			whitedb_record_to_userdata(pIterator->whitedb_record_metatable_ref, pIterator->pWhiteDb, pNext, 0, l );
		}
		else if (pIterator->iMode == 2)
		{
			pIterator->pParent = pNext;
			whitedb_record_to_userdata(pIterator->whitedb_record_metatable_ref,pIterator->pWhiteDb, pNext, 0, l );
		}
		else
		{
			assert(0);
		}
		return 1;
	}
	return 0;
}

//---------------------------------------------------------
static int whitedb_import_csv(lua_State *l)
{
	assert(lua_gettop(l) > 1 );
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	wg_int iResult = wg_import_db_csv(pInstance->pWhiteDb, (char*) lua_tostring(l, 2));
	lua_pushboolean(l, iResult == 0 ? 1 : 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_export_csv(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	wg_export_db_csv(pInstance->pWhiteDb, (char*)lua_tostring(l, 2));
	lua_pushboolean(l, 1 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_print(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	wg_print_db(pInstance->pWhiteDb);
	return 0;
}


//---------------------------------------------------------
static int remove_records_from_record( void* pWhiteDb, void* pRecord )
{
	int iSuccess = 1;
	int iFiledSize = wg_get_record_len( pWhiteDb, pRecord);
	for ( int iFieldIdx = 0; iFieldIdx < iFiledSize; iFieldIdx++ )
	{
		if ( wg_get_field_type( pWhiteDb, pRecord, iFieldIdx) == WG_RECORDTYPE  )
		{
			if ( wg_set_field( pWhiteDb, pRecord, iFieldIdx, 0) != 0 )
				iSuccess = 0;
		}
	}
	return iSuccess;
}


//---------------------------------------------------------
static int remove_record_from_parents( void* pWhiteDb, void* pRecord )
{
	int iSuccess = 1;
	void* pParent = wg_get_first_parent(  pWhiteDb, pRecord );
	while ( pParent )
	{
		int iFiledSize = wg_get_record_len( pWhiteDb, pParent);
		for ( int iFieldIdx = 0; iFieldIdx < iFiledSize; iFieldIdx++ )
		{
			if ( wg_get_field_type( pWhiteDb, pParent, iFieldIdx) == WG_RECORDTYPE  )
			{
				wg_int iFieldData  = wg_get_field( pWhiteDb, pParent, iFieldIdx );
				void* pFieldRecord = wg_decode_record( pWhiteDb, iFieldData );
				if ( pFieldRecord == pRecord )
				{
					if ( wg_set_field( pWhiteDb, pParent, iFieldIdx, 0) != 0 )
						iSuccess = 0;
				}
			}
		}
		pParent = wg_get_next_parent( pWhiteDb, pRecord, pParent);
	}
	return iSuccess;
}

//returns 0 on success
//	* returns -1 if the record is referenced by others and cannot be deleted.
//	* returns -2 on general error
//	* returns -3 on fatal error
//---------------------------------------------------------
static int whitedb_clear(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	
	void* wg_record = wg_get_first_record(pInstance->pWhiteDb);
	wg_int status = 0;
	void* pNext = NULL;
	while ( wg_record )
	{
		pNext =  wg_get_next_record(pInstance->pWhiteDb, wg_record );
		status = wg_delete_record(pInstance->pWhiteDb, wg_record );
		if ( status < 0 )
		{
			switch( status )
			{
				case -1:
					{
						if ( remove_record_from_parents( pInstance->pWhiteDb, wg_record ) )
							status = wg_delete_record(pInstance->pWhiteDb, wg_record );

						if ( status < 0 )
							luaL_error(l, "Fatal error cannot remove record from parents");
						break;
					}
				case -2:
				case -3:
				default:				
					{
						luaL_error(l, "Fatal error when removeing record");
						break;
					}
			}
		}

		wg_record = pNext;
	}
	return 0;
}
//---------------------------------------------------------
static int whitedb_records(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);

	whitedb_record_iterator* pRecIterator = (whitedb_record_iterator*)lua_newuserdata(l, sizeof(whitedb_record_iterator));
	pRecIterator->iMode = 1;
	pRecIterator->pParent = NULL;
	pRecIterator->pRecord = NULL;
	pRecIterator->pWhiteDb = pInstance->pWhiteDb;
	pRecIterator->whitedb_record_metatable_ref = pInstance->whitedb_record_metatable_ref;
	lua_pushcclosure(l, whitedb_records_iterator, 1);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_removec_records(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	assert(pRecord);
	lua_pushboolean(l, remove_records_from_record( pRecord->pWhiteDb, pRecord->pRecord));
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_field_size(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	assert(pRecord);
	wg_int iSize = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);
	if (iSize < 0)
		lua_pushnil(l);
	else
		lua_pushinteger(l, iSize);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_parents(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	assert(pRecord);

	whitedb_record_iterator* pRecordIterator = (whitedb_record_iterator*)lua_newuserdata(l, sizeof(whitedb_record_iterator));
	pRecordIterator->iMode = 2;
	pRecordIterator->pParent = NULL;
	pRecordIterator->pRecord = pRecord->pRecord;
	pRecordIterator->pWhiteDb = pRecord->pWhiteDb;
	pRecordIterator->whitedb_record_metatable_ref = pRecord->whitedb_record_metatable_ref;
	lua_pushcclosure(l, whitedb_records_iterator, 1);
	return 1;
}



//---------------------------------------------------------
static int whitedb_record_delete_first(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	void* pRecord = wg_get_first_record( pInstance->pWhiteDb );
	if ( pRecord )
	{
		wg_int iResult = wg_delete_record( pInstance->pWhiteDb, pRecord );
		lua_pushboolean(l, iResult == 0);
		return 1;
	}
	lua_pushboolean(l, 0);
	return 1;
}


//---------------------------------------------------------
static int whitedb_record_delete2(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 2);
	assert(pRecord);

	wg_int iSuccess = wg_delete_record(pRecord->pWhiteDb, pRecord->pRecord);
	lua_pushboolean(l, iSuccess == 0);
	lua_pushinteger(l, iSuccess);
	return 2;
}

//---------------------------------------------------------
static int whitedb_is_write_lock(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushboolean(l, pInstance->iLockWrite );
	return 1;
}


//---------------------------------------------------------
static int whitedb_is_read_lock(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushboolean(l, pInstance->iLockRead );
	return 1;
}

//---------------------------------------------------------
static int whitedb_read_start(lua_State *l)
{
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	if ( pInstance->iLockRead == 1 )
	{
		lua_pushboolean(l, true );
		assert( 0 );
		return 1;
	}

	wg_int wLock = wg_start_read(pInstance->pWhiteDb);
	if ( wLock != 0 )
	{
		pInstance->iLockRead = 1;
		pInstance->iLockReadTr = wLock;
		lua_pushboolean(l, 1 );
		return 1;
	}
	
	lua_pushboolean(l, 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_read_end(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	if ( pInstance->iLockRead == 0 )
	{
		lua_pushboolean(l, 0);
		//assert(0);
		return 1;
	}

	wg_int iWrite = wg_end_read(pInstance->pWhiteDb, pInstance->iLockReadTr );
	if ( iWrite == 1 )
	{
		pInstance->iLockRead = 0;
		pInstance->iLockReadTr = 0;
		lua_pushboolean(l, 1 );
		return 1;
	}
	lua_pushboolean(l, 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_write_start(lua_State *l)
{
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	
	if ( pInstance->iLockWrite == 1 )
	{
		assert(0);
		lua_pushboolean(l, 1);
		return 1;
	}

	wg_int iResult = wg_start_write(pInstance->pWhiteDb);

	if ( iResult != 0 )
	{
		pInstance->iLockWrite = 1;
		pInstance->iLockWriteTr = iResult;
		lua_pushboolean(l, 1);
		return 1;
	}

	lua_pushboolean(l,0);
	return 1;
}

//---------------------------------------------------------
static int whitedb_write_end(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	if ( pInstance->iLockWrite == 0 )
	{
		//assert(0);
		lua_pushboolean(l, 0 );
		return 1;
	}

	wg_int iResult = wg_end_write(pInstance->pWhiteDb, pInstance->iLockWriteTr );
	if ( iResult == 1 )
	{
		 pInstance->iLockWrite = 0;
		 pInstance->iLockWriteTr = 0;
		 lua_pushboolean(l, 1);
		 return 1;
	}

	lua_pushboolean(l, 0 );
	return 1;
}
//---------------------------------------------------------
static int whitedb_log_start(lua_State *l)
{
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
		lua_pushinteger(l, wg_start_logging(pInstance->pWhiteDb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_log_stop(lua_State *l)
{
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
		lua_pushinteger(l, wg_stop_logging(pInstance->pWhiteDb ));
	return 1;
}


//---------------------------------------------------------
static int whitedb_dump_export(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	int iResult = wg_dump(pInstance->pWhiteDb, (char*)lua_tostring(l, 2));
	lua_pushinteger(l, iResult == 0 ? 1 : 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_dump_import(lua_State *l) {
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance);
	int iResult = wg_import_dump(pInstance->pWhiteDb, (char*)lua_tostring(l, 1));
	lua_pushboolean(l, iResult == 0 ? 1 : 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_find_record(lua_State *l)
{
	whitedb_find_iterator* pIterator = (whitedb_find_iterator*)lua_touserdata(l, lua_upvalueindex(1));
	
	if (pIterator->iFieldType == LUA_TNONE)
		return 0;
	
	void* pRec = NULL;

	switch (pIterator->iFieldType )
	{
		case LUA_TNUMBER:
		{
			pRec = wg_find_record_double(pIterator->pWhiteDb, pIterator->iFieldIndex, pIterator->iFiendCond, pIterator->dValue, pIterator->pRecord);
			break;
		}
		case LUA_TSTRING:
		{
			pRec = wg_find_record_str(pIterator->pWhiteDb, pIterator->iFieldIndex, pIterator->iFiendCond, pIterator->sValue, pIterator->pRecord);
			break;
		}
		case LUA_TBOOLEAN:
		{
			pRec = wg_find_record_int(pIterator->pWhiteDb, pIterator->iFieldIndex, pIterator->iFiendCond, pIterator->bValue, pIterator->pRecord);
			break;
		}
		case LUA_TNIL:
		{
			pRec = wg_find_record_null(pIterator->pWhiteDb, pIterator->iFieldIndex, pIterator->iFiendCond, 0, pIterator->pRecord);
			break;
		}
		default:
			break;
	}

	if (pRec)
	{
		pIterator->pRecord = pRec;
		return whitedb_record_to_userdata(pIterator->whitedb_record_metatable_ref,pIterator->pWhiteDb, pRec, 0, l);
	}

	return 0;
}

//---------------------------------------------------------
static int whitedb_first(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	void* pRecord = wg_get_first_record(pInstance->pWhiteDb);
	if ( pRecord )
		return whitedb_record_to_userdata(pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, pRecord, 0 , l);

	lua_pushnil(l);
	return 1;
}

//---------------------------------------------------------
static int whitedb_next(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 2);
	assert( pRecord );
	void* pNextRecord = wg_get_next_record( pRecord->pWhiteDb, pRecord->pRecord );
	if ( pNextRecord )
		return whitedb_record_to_userdata(pRecord->whitedb_record_metatable_ref, pRecord->pWhiteDb , pNextRecord, 0, l );
	lua_pushnil(l);
	return 1;
}
//---------------------------------------------------------
static int whitedb_find(lua_State *l)
{
	assert(lua_gettop(l) > 3);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	int iColumn             = lua_tointeger(l, 2);
	const char* sCondition  = lua_tostring(l, 3);
	int iType               = lua_type(l, 4);
	int iCondition          = str_to_cond(sCondition);
	void* pRec              = NULL;

	whitedb_find_iterator* pFindIterator = (whitedb_find_iterator*)lua_newuserdata(l, sizeof(whitedb_find_iterator));
	
	pFindIterator->pWhiteDb = pInstance->pWhiteDb;
	pFindIterator->iFieldIndex = --iColumn;
	pFindIterator->iFieldType = iType;
	pFindIterator->iFiendCond = iCondition;
	pFindIterator->pRecord = NULL;
	pFindIterator->whitedb_record_metatable_ref = pInstance->whitedb_record_metatable_ref;

	switch (iType)
	{
		case LUA_TNUMBER:
		{
			pFindIterator->dValue = lua_tonumber(l, 4);
			break;
		}
		case LUA_TSTRING:
		{
			strcpy_s( pFindIterator->sValue, WHITEDB_MAX_FIND_STR_SIZE, lua_tostring(l, 4) );
			break;
		}
		case LUA_TBOOLEAN:
		{
			pFindIterator->bValue = lua_toboolean(l, 4) ? 1 : 0;
			break;
		}
		case LUA_TNIL:
		{
			break;
		}
		default:
			pFindIterator->iFieldType = LUA_TNONE;
			break;
	}

	lua_pushcclosure(l, whitedb_find_record, 1);
	return 1;
}

//---------------------------------------------------------
wg_int lua_value_to_wg(void* db, lua_State *l, int index)
{
	wg_int iResult = 0;
	int iType = lua_type(l, index);
	switch (iType)
	{
		case LUA_TNUMBER:
		{
			iResult = wg_encode_double(db, lua_tonumber(l, index));
			break;
		}
		case LUA_TSTRING:
		{
			iResult = wg_encode_str(db, (char*)lua_tostring(l, index), NULL);
			break;
		}
		case LUA_TBOOLEAN:
		{
			iResult = wg_encode_int(db, lua_tointeger(l, index) );
			break;
		}
		case LUA_TLIGHTUSERDATA:
		{
			void* pUserData = lua_touserdata(l, index);
			int iSize = lua_objlen(l, index);
			assert(iSize == sizeof(void*));
			iResult = wg_encode_blob(db,(char*) &pUserData, NULL, sizeof(void*)  );
		}
		case LUA_TUSERDATA:
			{				
				whitedb_record* pRecord = (whitedb_record*)luaL_checkudata(l, index, WHITEDB_RECORD_METATABLE);
				if ( pRecord )
					iResult = wg_encode_record(db, pRecord->pRecord );
				else
					iResult = wg_encode_null(db, 0);
				break;
			}
		case LUA_TNIL:
		default:
			iResult = wg_encode_null(db, 0);
			break;
	}
	return iResult;
}

//---------------------------------------------------------
static int whitedb_record_create_t(lua_State *l)
{
	assert(lua_gettop(l) > 1);

	whitedb_instance* pInstance = (whitedb_instance*)lua_touserdata(l, 1);
	assert(pInstance);
	assert(pInstance->pWhiteDb);
	int               iSize = 1;
	if ( lua_type(l,2) != LUA_TTABLE )
	{
		lua_pushnil(l);
		return 1;
	}
	iSize = lua_objlen(l,2);
	if ( iSize < 1)
	{
		lua_pushnil(l);
		return 1;		
	}
	void* pRecord = wg_create_record( pInstance->pWhiteDb, iSize );
	int   iRec = 0;
	for (int iIndex = 1; iIndex <= iSize; iIndex++ )
	{		
		lua_rawgeti(l, 2, iIndex );
		wg_int wg_data = lua_value_to_wg(pInstance->pWhiteDb, l, -1);
		wg_set_field(pInstance->pWhiteDb, pRecord, iRec, wg_data);
		iRec++;
		lua_pop(l, 1);
	}

	return whitedb_record_to_userdata(pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, pRecord, iSize, l);
}

//---------------------------------------------------------
// find db, key str
static int whitedb_find_key(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	const char* pStrFieldName = lua_tostring(l,2);
	void* pRecord = wg_get_first_record( pInstance->pWhiteDb );
	while ( pRecord )
	{
		if ( wg_get_field_type(pInstance->pWhiteDb,pRecord,0) == WG_STRTYPE )
		{
			char* strField = wg_decode_str( pInstance->pWhiteDb,  wg_get_field( pInstance->pWhiteDb, pRecord, 0));
			if ( strcmp( strField, pStrFieldName) == 0 )
			{
				return whitedb_record_to_userdata(pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, pRecord, 0, l);
			}
		}
		pRecord = wg_get_next_record( pInstance->pWhiteDb, pRecord);
	}
	lua_pushnil(l);
	return 1;
}


//---------------------------------------------------------
// find db, index, cond, value
static int whitedb_find_one(lua_State *l) {
	assert(lua_gettop(l) > 3);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	int               iFieldIndex = lua_tointeger(l, 2);
	const char*       sCondition  = lua_tostring(l, 3);
	int               iType       = lua_type(l, 4);
	int               iCondition  = str_to_cond(sCondition);
	void*             pRec        = NULL;
	iFieldIndex--;
	switch (iType)
	{
		case LUA_TNUMBER:
		{
			pRec = wg_find_record_double(pInstance->pWhiteDb, iFieldIndex, iCondition, lua_tonumber(l, 4), NULL);
			break;
		}
		case LUA_TSTRING:
		{
			pRec = wg_find_record_str(pInstance->pWhiteDb, iFieldIndex, iCondition, (char*)lua_tostring(l, 4), NULL);
			break;
		}
		case LUA_TBOOLEAN:
		{
			pRec = wg_find_record_int(pInstance->pWhiteDb, iFieldIndex, iCondition, lua_toboolean(l, 4) ? 1 : 0 , NULL);
			break;
		}
		case LUA_TNIL:
		{
			pRec = wg_find_record_null(pInstance->pWhiteDb, iFieldIndex, iCondition, 0, NULL);
			break;
		}
		default:
			break;
	}
	
	if (pRec)
		whitedb_record_to_userdata( pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, pRec, 0, l);
	else
		lua_pushnil(l);

	return 1;
}

//---------------------------------------------------------
static int whitedb_size(lua_State *l) {
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushnumber(l, wg_database_size(pInstance->pWhiteDb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_free_size(lua_State *l) {
	assert(lua_gettop(l) > 0);

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushnumber(l, wg_database_freesize(pInstance->pWhiteDb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_log_replay(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushinteger(l, wg_replay_log(pInstance->pWhiteDb, (char*)lua_tostring(l,2) ));
	return 1;
}

//---------------------------------------------------------
// 
static int whitedb_index_create(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	int iFieldIndex = lua_tointeger(l, 2);
	iFieldIndex--;
	assert( iFieldIndex >= 0);
	if (wg_column_to_index_id(pInstance->pWhiteDb, iFieldIndex, WG_INDEX_TYPE_TTREE, NULL, 0) == -1)
		lua_pushboolean(l, wg_create_index(pInstance->pWhiteDb, iFieldIndex, WG_INDEX_TYPE_TTREE, NULL, 0) == 0 ? 1 : 0 );
	else
		lua_pushboolean(l, 0 );

	return 1;
}

//---------------------------------------------------------
// db, index, table
static int whitedb_index_multi(lua_State *l) {
	assert(lua_gettop(l) > 2);
	if (lua_type(l, 3) != LUA_TTABLE )
	{
		lua_pushboolean(l, 0);
		return 1;
	}
	
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	wg_int iFieldIndex = lua_tointeger(l, 2);
	wg_int record_index = 0;
	wg_int Match_rec[DWhiteDbMaxMultiIndexSize];
	iFieldIndex--;
	assert( iFieldIndex >= 0);
	lua_pushnil(l);
	while ( lua_next(l, 3) != 0 )
	{
		Match_rec[record_index] = lua_value_to_wg(pInstance->pWhiteDb, l, -1);
		record_index++;
		lua_pop(l, 1);
	}
		
	wg_int result = wg_create_index(pInstance->pWhiteDb, iFieldIndex, WG_INDEX_TYPE_TTREE, Match_rec, record_index);
	lua_pushboolean(l, result == 0 ? 0 : 1);
	return 1;
}

//---------------------------------------------------------
static int whitedb_query_record(lua_State *l)
{
	assert(lua_gettop(l) > 0);

	whitedb_query_iterator* pIterator = (whitedb_query_iterator*)lua_touserdata(l, lua_upvalueindex(1));
	void* pRecord = wg_fetch(pIterator->pWhiteDb, pIterator->pQuery);
	if (pRecord)
		return whitedb_record_to_userdata(pIterator->whitedb_record_metatable_ref, pIterator->pWhiteDb, pRecord, 0, l);

	wg_free_query(pIterator->pWhiteDb, pIterator->pQuery);
	return 0;
}

//---------------------------------------------------------
static void calc_query_param(void* db, wg_query_arg* arg, lua_State* l, int iIndex )
{
	lua_pushnil(l);
	while (lua_next(l, iIndex))
	{
		if (lua_type(l, -2) == LUA_TSTRING)
		{
			const char *key = lua_tostring(l, -2);
			
			if (strcmp(key, "cond") == 0)
				arg->cond =  str_to_cond( lua_tostring( l, -1) );

			if (strcmp(key, "value") == 0)
				arg->value = lua_value_to_wg(db, l, -1);

			if (strcmp(key, "column") == 0)
				arg->column = lua_tointeger(l, -1) - 1;

		}
		lua_pop(l, 1);
	}
}


//---------------------------------------------------------
// db, table
static int whitedb_query_t(lua_State *l) {

	assert(lua_gettop(l) > 1 );
	if (lua_type(l, 2) != LUA_TTABLE || lua_objlen(l, 2) == 0)
		return 0;

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

		wg_int iQuery_size = 0;

	wg_query_arg Query_arg_list[DWhiteDbMaxQuerySize];
	wg_query* Query = NULL;

	lua_pushnil(l);
	while (lua_next(l, 2) != 0)
	{
		if ( lua_type(l, -1) == LUA_TTABLE )
		{
			calc_query_param(pInstance->pWhiteDb, &Query_arg_list[iQuery_size], l, lua_gettop(l));
			iQuery_size++;
		}
		lua_pop(l, 1);
	}

	Query = wg_make_query( pInstance->pWhiteDb, NULL, 0, Query_arg_list, iQuery_size);
	lua_newtable(l);
	int counter = 1;
	if (Query)
	{
		void* pRecord = wg_fetch(pInstance->pWhiteDb, Query);
		while ( pRecord )
		{
			lua_pushinteger(l,counter);
			
			whitedb_record* pNewIntRecord = (whitedb_record*)lua_newuserdata(l, sizeof(whitedb_record));
			pNewIntRecord->pRecord  = pRecord;
			pNewIntRecord->pWhiteDb = pInstance->pWhiteDb;
		
			lua_rawgeti(l, LUA_REGISTRYINDEX, pInstance->whitedb_record_metatable_ref );
			lua_setmetatable(l, -2);

			lua_settable(l, -3);
			counter++;
			pRecord = wg_fetch(pInstance->pWhiteDb, Query);
		}

		wg_free_query( pInstance->pWhiteDb, Query );
	}
	return 1;
}

//---------------------------------------------------------
// db, table
static int whitedb_query(lua_State *l) {

	assert(lua_gettop(l) > 1 );
	if (lua_type(l, 2) != LUA_TTABLE || lua_objlen(l, 2) == 0)
		return 0;

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	wg_int iQuery_size = 0;

	wg_query_arg Query_arg_list[DWhiteDbMaxQuerySize];
	wg_query* Query = NULL;

	lua_pushnil(l);
	while (lua_next(l, 2) != 0)
	{
		if ( lua_type(l, -1) == LUA_TTABLE )
		{
			calc_query_param(pInstance->pWhiteDb, &Query_arg_list[iQuery_size], l, lua_gettop(l));
			iQuery_size++;
		}
		lua_pop(l, 1);
	}

	Query = wg_make_query( pInstance->pWhiteDb, NULL, 0, Query_arg_list, iQuery_size);
	if (Query)
	{
		whitedb_query_iterator* pQuery_iterator = (whitedb_query_iterator*)lua_newuserdata(l, sizeof(whitedb_query_iterator));
		pQuery_iterator->pQuery = Query;
		pQuery_iterator->pWhiteDb = pInstance->pWhiteDb;
		pQuery_iterator->whitedb_record_metatable_ref = pInstance->whitedb_record_metatable_ref;
		lua_pushcclosure(l, whitedb_query_record, 1);
		return 1;
	}

	return 0;
}

//---------------------------------------------------------
static int whitedb_count(lua_State *l)
{
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	int iCounter = 0;
	void* pRecord = wg_get_first_record(pInstance->pWhiteDb);
	while ( pRecord) 
	{
		pRecord = wg_get_next_record(pInstance->pWhiteDb, pRecord );
		iCounter++;
	}
	lua_pushinteger(l, iCounter);
	return 1;
}

//---------------------------------------------------------
static int whitedb_query_sum_t(lua_State *l) 
{
	int iTop = lua_gettop(l);
	assert(iTop > 2 );

	if (lua_type(l, 2) != LUA_TTABLE || lua_objlen(l, 2) == 0)
	{
		lua_pushnil(l);
		return 1;
	}

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	wg_int iQuery_size = 0;
	wg_query_arg Query_arg_list[DWhiteDbMaxQuerySize];
	wg_query* Query = NULL;

	int iRecordCount = 0;
	int iSumField    = lua_tointeger(l,3);
	double dSumValue = 0;
	double dSumLimit = 0.f;

	assert( iSumField > 0 );
	iSumField--;
	if ( iTop > 3 )
		dSumLimit = lua_tonumber(l,4);

	lua_pushnil(l);
	while (lua_next(l, 2) != 0)
	{
		if ( lua_type(l, -1) == LUA_TTABLE )
		{
			calc_query_param(pInstance->pWhiteDb, &Query_arg_list[iQuery_size], l, lua_gettop(l));
			iQuery_size++;
		}
		lua_pop(l, 1);
	}

	void* pRecord = NULL;
	Query = wg_make_query( pInstance->pWhiteDb, NULL, 0, Query_arg_list, iQuery_size);
	lua_newtable(l);
	if (Query)
	{
		pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		while ( pRecord )
		{
			iRecordCount++;
			wg_int iFieldType = wg_get_field_type(pInstance->pWhiteDb, pRecord, iSumField);
			if ( iFieldType == WG_INTTYPE)
				dSumValue += wg_decode_int( pInstance->pWhiteDb, wg_get_field( pInstance->pWhiteDb, pRecord, iSumField ));
			else if ( iFieldType == WG_DOUBLETYPE)
				dSumValue += wg_decode_double( pInstance->pWhiteDb, wg_get_field( pInstance->pWhiteDb, pRecord, iSumField ));
			
			if ( dSumValue >= dSumLimit )
				break;

			lua_pushinteger(l,iRecordCount);
			whitedb_record_to_userdata( pInstance->whitedb_record_metatable_ref, pInstance->pWhiteDb, pRecord, 0, l);
			lua_settable(l, -3);
			pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		}
		wg_free_query( pInstance->pWhiteDb, Query );
	}
	return 1;
}



//---------------------------------------------------------
static int whitedb_query_count_sum(lua_State *l) {

	assert(lua_gettop(l) > 2 );
	if (lua_type(l, 2) != LUA_TTABLE || lua_objlen(l, 2) == 0)
	{
		lua_pushinteger(l,0);
		lua_pushinteger(l,0);
		return 2;
	}

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	wg_int iQuery_size = 0;
	wg_query_arg Query_arg_list[DWhiteDbMaxQuerySize];
	wg_query* Query = NULL;

	int iRecordCount = 0;
	int iSumField    = lua_tointeger(l,3);
	double dSumValue = 0;
	double dSumLimit = 0;

	assert( iSumField > 0 );
	iSumField--;

	lua_pushnil(l);
	while (lua_next(l, 2) != 0)
	{
		if ( lua_type(l, -1) == LUA_TTABLE )
		{
			calc_query_param(pInstance->pWhiteDb, &Query_arg_list[iQuery_size], l, lua_gettop(l));
			iQuery_size++;
		}
		lua_pop(l, 1);
	}
	void* pRecord = NULL;
	Query = wg_make_query( pInstance->pWhiteDb, NULL, 0, Query_arg_list, iQuery_size);
	if (Query)
	{
		pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		while ( pRecord )
		{
			iRecordCount++;
			wg_int iFieldType = wg_get_field_type(pInstance->pWhiteDb, pRecord, iSumField);
			if ( iFieldType == WG_INTTYPE)
				dSumValue += wg_decode_int( pInstance->pWhiteDb, wg_get_field( pInstance->pWhiteDb, pRecord, iSumField ));
			else if ( iFieldType == WG_DOUBLETYPE)
				dSumValue += wg_decode_double( pInstance->pWhiteDb, wg_get_field( pInstance->pWhiteDb, pRecord, iSumField ));
			pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		}
		wg_free_query( pInstance->pWhiteDb, Query );
	}
	lua_pushinteger( l , iRecordCount );
	lua_pushnumber( l , dSumValue );
	return 2;
}

//---------------------------------------------------------
// db, table
static int whitedb_query_count(lua_State *l) {

	assert(lua_gettop(l) > 1 );
	if (lua_type(l, 2) != LUA_TTABLE || lua_objlen(l, 2) == 0)
	{
		lua_pushinteger(l,0);
		return 1;
	}

	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)

	wg_int iQuery_size = 0;
	wg_query_arg Query_arg_list[DWhiteDbMaxQuerySize];
	wg_query* Query = NULL;

	int iRecordCount = 0;

	lua_pushnil(l);
	while (lua_next(l, 2) != 0)
	{
		if ( lua_type(l, -1) == LUA_TTABLE )
		{
			calc_query_param(pInstance->pWhiteDb, &Query_arg_list[iQuery_size], l, lua_gettop(l));
			iQuery_size++;
		}
		lua_pop(l, 1);
	}
	void* pRecord = NULL;
	Query = wg_make_query( pInstance->pWhiteDb, NULL, 0, Query_arg_list, iQuery_size);
	if (Query)
	{
		pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		while ( pRecord )
		{
			iRecordCount++;
			pRecord = wg_fetch(pInstance->pWhiteDb, Query );
		}
		wg_free_query( pInstance->pWhiteDb, Query );
	}

	lua_pushinteger( l , iRecordCount );
	return 1;
}

//---------------------------------------------------------
// 
static int whitedb_index_drop(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* pInstance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(pInstance)
	lua_pushboolean( l, wg_drop_index(pInstance->pWhiteDb, lua_tointeger(l, 2) ) == 0 ? 1 : 0 );
	return 1;
}

//---------------------------------------------------------
static int whitedb_gc(lua_State *l)
{
	whitedb_instance* pInstance = check_instance(l,1);
	assert(pInstance);
	assert(pInstance->pWhiteDb);

	if (pInstance)
	{
		if ( pInstance->iMode == 3 ) // existing
			wg_detach_database(pInstance->pWhiteDb);
		else if (pInstance->iMode == 2) // local db
		{
			wg_detach_database( pInstance->pWhiteDb);
			wg_delete_local_database(pInstance->pWhiteDb);
		}
		else
		{
			wg_detach_database( pInstance->pWhiteDb);
			wg_delete_database(pInstance->sName);
		}
	}
	return 0;
}


//------------------------------------------------------------------------
// Attach whitedb database
// Parameters : name, size, mode [ nil | 0, 1 logged, 2 localdb, 3 existing ], permission
// Return     : nil or database pointer, metatable WHITEDB_METATABLE
//
// Linux check shared memory size
// cat /proc/sys/kernel/shmmax
// echo shared_memory_size > /proc/sys/kernel/shmmax
//-------------------------------------------------------------------------

static int whitedb_attach(lua_State *l) {

	assert(lua_gettop(l) > 0);

	int iParamCount = lua_gettop(l);

	const char* sName = iParamCount > 0 ? lua_tostring (l, 1) : NULL;
	double fSize      = iParamCount > 1 ? lua_tonumber(l, 2) : 0;
	unsigned long int iSize= (unsigned long int) fSize; // white db bug, not handled db attach params correctly
	
	int   iMode       = iParamCount > 2 ? lua_tointeger(l, 3) : 0;
	int	  iPermission = iParamCount > 3 ? lua_tointeger(l, 4) : 0;
	void* pDb          = NULL;

	if (strlen(sName) == 0)
	{
		luaL_error(l, "Invalid database name");
		return 0;
	}

	if (iMode == 2)
	{
		pDb = wg_attach_local_database(iSize);
	}
	else if (iMode == 1)
	{
		if (iPermission != 0)
			pDb = wg_attach_logged_database_mode((char*)sName, iSize, iMode);
		else
			pDb = wg_attach_logged_database((char*)sName, iSize);
	}
	else if (iMode == 3)
		pDb = wg_attach_existing_database((char*)sName);
	else
	{
		if (iMode != 0)
			pDb = wg_attach_database_mode((char*)sName, iSize, iMode);
		else
			pDb = wg_attach_database((char*)sName, iSize);
	}

	if (pDb)
	{
		whitedb_instance* pInstance = (whitedb_instance*)lua_newuserdata(l, sizeof(whitedb_instance));
		pInstance->iMode = iMode;
		pInstance->iPermissions = iPermission;
		pInstance->pWhiteDb = pDb;
		pInstance->iLockRead = 0;
		pInstance->iLockReadTr = 0;
		pInstance->iLockWrite = 0;
		pInstance->iLockWriteTr;
		strcpy_s(pInstance->sName, DWhiteDbNameSize, sName);

		luaL_getmetatable(l, WHITEDB_RECORD_METATABLE);
		pInstance->whitedb_record_metatable_ref = luaL_ref(l, LUA_REGISTRYINDEX);

		luaL_getmetatable(l, WHITEDB_METATABLE );
		lua_setmetatable(l, -2);
	
	}
	else
		lua_pushnil(l);

	return 1;
}



//-------------------------------------------------------------------------

static int whitedb_record_field_set_t(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	assert(pRecord);

	if (lua_type(l, 2) != LUA_TTABLE)
	{
		lua_pushinteger(l,0);
		return 1;
	}

	wg_int iLen = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);
	int    iRec = 0;
	for (int iIndex = 1; iIndex <= iLen; iIndex++ )
	{		
		lua_rawgeti(l, 2, iIndex );
		if (!lua_isnil(l, -1))
		{

			wg_int wg_data = lua_value_to_wg(pRecord->pWhiteDb, l, -1);
			if ( wg_set_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex - 1, wg_data) == 0 )
				iRec++;
		}
		lua_pop(l, 1);
	}

	lua_pushinteger(l, iRec);
	return 1;
}


//-------------------------------------------------------------------------
static int whitedb_record_field_rec_ref(lua_State *l)
{
	assert(lua_gettop(l) > 2);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	int             iIndex  = lua_tointeger(l, 2);
	int             iType   = lua_type(l, 3);
	
	if ( iType != LUA_TUSERDATA )
	{
		lua_pushboolean(l, 0 );
		return 1;
	}

	assert(pRecord);
	wg_int          iLen = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);
	iIndex--;
	if (iIndex > iLen)
	{
#ifdef _DEBUG
		luaL_error(l, "Invalid field index %d", iIndex);
#else
		lua_pushboolean(l, 0);
		return 1;
#endif
	}
	
#ifdef _DEBUG
	whitedb_record* pRefRecord = (whitedb_record*) luaL_checkudata(l, 3, WHITEDB_RECORD_METATABLE);
#else
	whitedb_record* pRefRecord = (whitedb_record*) lua_touserdata(l, 3);
#endif
	assert( pRefRecord->pWhiteDb == pRecord->pWhiteDb );
	wg_int          iData   = wg_encode_record( pRecord->pWhiteDb, pRefRecord->pRecord);
	wg_int          iResult = 0;	

	iResult = wg_set_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex, iData);
	lua_pushboolean(l, iResult == 0);
	return 1;
}

//-------------------------------------------------------------------------
static int wg_value_to_lua( lua_State *l, int iFieldType, void* pWhiteDb, wg_int pField, int whitedb_record_metatable_ref )
{
	int iResult = 0;

	switch (iFieldType)
	{
	case WG_INTTYPE:
		{
			int bValue = wg_decode_int(pWhiteDb, pField);
			lua_pushboolean(l, bValue);
			iResult = 1;
			break;
		}
	case WG_DOUBLETYPE:
		{
			double dValue = wg_decode_double(pWhiteDb, pField);
			lua_pushnumber(l, dValue);
			iResult = 1;
			break;
		}

	case WG_STRTYPE:
		{
			char* sValue = wg_decode_str(pWhiteDb, pField);
			lua_pushstring(l, sValue);
			iResult = 1;
			break;
		}
	case WG_BLOBTYPE:
		{
			int    iSize = wg_decode_blob_len(pWhiteDb, pField);
			assert(iSize == sizeof(void*));
			void** pData = (void**) wg_decode_blob(pWhiteDb, pField);
			lua_pushlightuserdata(l, *pData);
			iResult = 1;
			break;
		}
	case WG_RECORDTYPE:
		{
			void* pData = wg_decode_record(pWhiteDb, pField);
			whitedb_record_to_userdata( whitedb_record_metatable_ref, pWhiteDb, pData,0,l);
			iResult = 1;
			break;
		}
	case WG_NULLTYPE:
	default:
		lua_pushnil(l);
		iResult = 1;
		break;
	}
	return iResult;
}

//-------------------------------------------------------------------------
void* find_whitedb_kv_record( void* pWhiteDb, void* pRecord, const char* strName )
{
	int iRecSize = wg_get_record_len( pWhiteDb, pRecord );
	for ( wg_int iIndex = 0; iIndex < iRecSize; iIndex ++ )
	{
		if ( wg_get_field_type( pWhiteDb, pRecord, iIndex ) == WG_RECORDTYPE )
		{
			void* pCurrRecord = wg_decode_record( pWhiteDb, wg_get_field(pWhiteDb, pRecord, iIndex));
			if ( pCurrRecord && wg_get_record_len( pWhiteDb, pCurrRecord) >= 2 && wg_get_field_type( pWhiteDb, pCurrRecord, 0 ) == WG_STRTYPE )
			{
				char* strField = wg_decode_str( pWhiteDb,  wg_get_field( pWhiteDb, pCurrRecord, 0));
				if ( strcmp( strField, strName) == 0 )
				{
					return pCurrRecord;
				}
			}
		}
	}
	return NULL;
}

//-------------------------------------------------------------------------
wg_int find_whitedb_kv_free_field( void* pWhiteDb, void* pRecord )
{
	int iRecSize = wg_get_record_len( pWhiteDb, pRecord );
	for ( wg_int iIndex = 0; iIndex < iRecSize; iIndex ++ )
	{
		if ( wg_get_field_type( pWhiteDb, pRecord, (wg_int) iIndex ) == WG_NULLTYPE )
		{
			return iIndex;
		}
	}
	return -1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_get_kv_t(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord   = (whitedb_record*) lua_touserdata(l, 1);
	lua_newtable(l);
	int iRecSize = wg_get_record_len( pRecord->pWhiteDb, pRecord->pRecord );
	for ( wg_int iIndex = 0; iIndex < iRecSize; iIndex ++ )
	{
		if ( wg_get_field_type( pRecord->pWhiteDb, pRecord->pRecord, iIndex ) == WG_RECORDTYPE )
		{
			void* pCurrRecord = wg_decode_record( pRecord->pWhiteDb, wg_get_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex));
			if ( pCurrRecord && wg_get_record_len( pRecord->pWhiteDb, pCurrRecord) == 2 && wg_get_field_type( pRecord->pWhiteDb, pCurrRecord, 0 ) == WG_STRTYPE )
			{
				char* strField = wg_decode_str( pRecord->pWhiteDb,  wg_get_field( pRecord->pWhiteDb, pCurrRecord, 0));
				lua_pushstring(l, strField);
				wg_int iFieldValue = wg_get_field_type( pRecord->pWhiteDb, pCurrRecord, 1 );
				wg_int iField      = wg_get_field(pRecord->pWhiteDb, pCurrRecord, 1 );
				wg_value_to_lua(l, iFieldValue, pRecord->pWhiteDb, iField, pRecord->whitedb_record_metatable_ref );
				lua_settable(l, -3);
			}
		}
	}
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_get_kv(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord   = (whitedb_record*) lua_touserdata(l, 1);
	const char*     strName   = lua_tostring(l, 2);
	int             iType     = lua_type(l, 3);
	void*           pKVRecord = find_whitedb_kv_record( pRecord->pWhiteDb, pRecord->pRecord, strName );
	
	if ( pKVRecord == NULL )
	{
		lua_pushnil(l);
		return 1;
	}
	wg_int iFieldType = wg_get_field_type(pRecord->pWhiteDb, pKVRecord, 1);
	wg_int pField = wg_get_field(pRecord->pWhiteDb, pKVRecord, 1);
	wg_int pTimeField = wg_get_field(pRecord->pWhiteDb, pKVRecord, 2);


	if ( pField == 0 || pField == WG_ILLEGAL)
	{
		lua_pushnil(l);
		return 1;
	}

	return wg_value_to_lua(l, iFieldType, pRecord->pWhiteDb, pField, pRecord->whitedb_record_metatable_ref);
}

//-------------------------------------------------------------------------
static inline int whitedb_set_kv_value( void* pWhiteDb, void* pRecord, const char* strName, lua_State *l, int iIndex  )
{
	void* pKVRecord = find_whitedb_kv_record( pWhiteDb, pRecord, strName );
	if ( !pKVRecord )
	{
		wg_int iField = find_whitedb_kv_free_field( pWhiteDb, pRecord );
		if ( iField == -1 )
			return 0;

		pKVRecord = wg_create_record(pWhiteDb, 2);
		wg_int iNameResult = wg_set_str_field(pWhiteDb, pKVRecord, 0, (char*) strName );
		wg_int iRecord     = wg_encode_record(pWhiteDb, pKVRecord );
		wg_int iResult     = wg_set_field( pWhiteDb, pRecord, iField, iRecord );
		if ( iResult != 0 || iNameResult != 0 )
		{
			wg_delete_record(pWhiteDb, pKVRecord);
			return 0;
		}
	}

	wg_int iData     = lua_value_to_wg( pWhiteDb, l, iIndex);
	wg_int iSetField = wg_set_field( pWhiteDb, pKVRecord, 1, iData);
	return 1;
}



//-------------------------------------------------------------------------
static int whitedb_record_field_set_kv_t(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord   = (whitedb_record*) lua_touserdata(l, 1);
	int             iType     = lua_type(l, 2);
	if ( iType != LUA_TTABLE)
	{
		lua_pushboolean(l,0);
		return 1;
	}
	
	lua_pushnil(l);
	int iProc = 0;
	while ( lua_next(l, 2) != 0 )
	{
#ifdef _DEBUG
		iType     = lua_type(l, -2);                  // value type
		assert( iType == LUA_TSTRING );
#endif
		iType = lua_type(l, -1);                     // value type
		const char* strName = lua_tostring(l,-2);
		if ( iType == LUA_TTABLE || iType == LUA_TFUNCTION || iType == LUA_TTHREAD )
		{
			lua_pop(l, 1);
			continue;
		}
		int iSuccess = whitedb_set_kv_value( pRecord->pWhiteDb, pRecord->pRecord, strName, l, -1  );
		lua_pop(l, 1);
		iProc++;
	}

	lua_pushboolean(l, iProc > 0 ? 1 : 0 );
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_set_kv(lua_State *l)
{
	assert(lua_gettop(l) > 2);
	whitedb_record* pRecord   = (whitedb_record*) lua_touserdata(l, 1);
	const char*     strName   = lua_tostring(l, 2);
	int iSuccess = whitedb_set_kv_value( pRecord->pWhiteDb, pRecord->pRecord, strName, l, 3  );
	lua_pushboolean(l, iSuccess );
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_set(lua_State *l)
{
	assert(lua_gettop(l) > 2);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	int             iIndex  = lua_tointeger(l, 2);
	int             iType   = lua_type(l, 3);
	assert(pRecord);
	wg_int          iData = 0;
	wg_int          iLen = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);
	wg_int          iResult = 0;
	iIndex--;
	if (iIndex > iLen)
	{
#ifdef _DEBUG
		luaL_error(l, "Invalid field index %d", iIndex);
#else
		lua_pushboolean(l, 0);
		return 1;
#endif
	}
	iData = lua_value_to_wg(pRecord->pWhiteDb, l, 3);
	iResult = wg_set_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex, iData);
	lua_pushboolean(l, iResult == 0);
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_new(lua_State *l)
{
	assert( lua_gettop(l) > 1 );
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	wg_int          iIndex  = lua_tointeger(l, 2);
	int             iSize   = lua_tointeger(l, 3);

	assert(pRecord);
	assert(pRecord->pWhiteDb);
	iIndex--;

	void* pNewRecord = wg_create_record( pRecord->pWhiteDb, iSize);
	wg_int newrec = wg_encode_record( pRecord->pWhiteDb, pNewRecord);
	wg_int field  = wg_set_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex, newrec );

	return whitedb_record_to_userdata(pRecord->whitedb_record_metatable_ref, pRecord->pWhiteDb, pNewRecord, iSize, l );
}

//-------------------------------------------------------------------------
static int whitedb_record_print(lua_State *l)
{
	assert(lua_gettop(l) > 0);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	wg_print_record(pRecord->pWhiteDb, (wg_int*)pRecord->pRecord);
	return 0;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_type_s(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	wg_int          iRecord = lua_tointeger(l, 2);
	iRecord--;
	wg_int iFiledType = wg_get_field_type (pRecord->pWhiteDb, pRecord->pRecord, iRecord) ;
	switch (iFiledType)
	{
	case WG_INTTYPE:
		{
			lua_pushstring(l, "boolean");
			break;
		}
	case WG_DOUBLETYPE:
		{
			lua_pushstring(l, "number");
			break;
		}

	case WG_STRTYPE:
		{
			lua_pushstring(l, "string");
			break;
		}
	case WG_BLOBTYPE:
		{
			lua_pushstring(l, "userdata");
			break;
		}
	case WG_RECORDTYPE:
		{
			lua_pushstring(l, "record");
			break;
		}
	case WG_NULLTYPE:
		{
			lua_pushstring(l, "record");
			break;
		}
	default:
		lua_pushstring(l, "nil");
		break;
	}
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_type(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord = (whitedb_record*)lua_touserdata(l, 1);
	wg_int          iRecord = lua_tointeger(l, 2);
	iRecord--;
	lua_pushinteger( l,  wg_get_field_type (pRecord->pWhiteDb, pRecord->pRecord, iRecord) );
	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_get_t(lua_State *l)
{
	int iTop = lua_gettop(l);
	assert(iTop > 0);

	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	assert(pRecord);
	wg_int iSize = 0;
	wg_int iFieldType = 0;
	wg_int pField = 0;
	int    iResult = 0;

	if (iTop < 2)
	{
		lua_newtable(l);
		iResult = 1;
	}

	wg_int iLen = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);

	for (int iIndex = 1; iIndex <= iLen; iIndex++)
	{
		iFieldType = wg_get_field_type(pRecord->pWhiteDb, pRecord->pRecord, iIndex - 1);
		pField = wg_get_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex - 1);
		iSize = wg_value_to_lua(l, iFieldType, pRecord->pWhiteDb, pField, pRecord->whitedb_record_metatable_ref);
		if (iSize)
			lua_rawseti(l, -2, iIndex);
	}
	return iResult;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_get(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* pRecord = (whitedb_record*) lua_touserdata(l, 1);
	int             iIndex = lua_tointeger(l, 2);

	assert(pRecord);
	wg_int          iData = 0;
	wg_int          iLen = wg_get_record_len(pRecord->pWhiteDb, pRecord->pRecord);
	wg_int          iResult = 0;
	iIndex--;
	if (iIndex > iLen)
	{
		lua_pushnil(l);
		return 1;
	}
	
	wg_int iFieldType = wg_get_field_type(pRecord->pWhiteDb, pRecord->pRecord, iIndex);
	wg_int pField = wg_get_field(pRecord->pWhiteDb, pRecord->pRecord, iIndex);

	if ( pField == WG_ILLEGAL)
	{
#ifdef _DEBUG		
		luaL_error(l, "Invalid field value! Field type : %d", iFieldType );
#else
		lua_pushnil(l);
		return 1;
#endif
	}
	return wg_value_to_lua(l, iFieldType, pRecord->pWhiteDb, pField, pRecord->whitedb_record_metatable_ref);
}

//---------------------------------------------------------
static const struct luaL_Reg lib_whitedb_record_meta[] =
{
	{ "delete",		whitedb_record_delete },
	{ "parents",	whitedb_record_parents },
	{ "size",    	whitedb_record_field_size },
	{ "set",        whitedb_record_field_set },
	{ "get",        whitedb_record_field_get },
	{ "set_t",      whitedb_record_field_set_t },
	{ "get_t",      whitedb_record_field_get_t },
	{ "set_kv",     whitedb_record_field_set_kv },
	{ "set_kv_t",   whitedb_record_field_set_kv_t },
	{ "get_kv",     whitedb_record_field_get_kv },
	{ "get_kv_t",   whitedb_record_field_get_kv_t },
	{ "rec_ref",    whitedb_record_field_rec_ref },
	{ "type",       whitedb_record_field_type },
	{ "type_s",     whitedb_record_field_type_s },
	{ "record",     whitedb_record_new },
	{ "del_recs",   whitedb_record_removec_records },
	{ "print",      whitedb_record_print },
	{ NULL, NULL }
};

//---------------------------------------------------------
static const struct luaL_Reg lib_whitedb[] =
{
	{ "attach",				whitedb_attach },
	{ NULL, NULL }
};

//---------------------------------------------------------
static const struct luaL_Reg lib_whitedb_meta[] =
{
	{ "record",         whitedb_record_create },
	{ "record_t",       whitedb_record_create_t },
	{ "delete",         whitedb_record_delete2 },
	{ "delete_first",   whitedb_record_delete_first },
	{ "records",	    whitedb_records },
	{ "is_read_lock",   whitedb_is_read_lock },
	{ "is_write_lock",  whitedb_is_write_lock },
	{ "read_start",     whitedb_read_start },
	{ "read_end",       whitedb_read_end   },
	{ "write_start",    whitedb_write_start },
	{ "write_end",      whitedb_write_end },
	{ "log_start",      whitedb_log_start },
	{ "log_stop",       whitedb_log_stop},
	{ "log_replay",     whitedb_log_replay },
	{ "size",           whitedb_size },
	{ "find_k",         whitedb_find_key },
	{ "find_one",       whitedb_find_one },
	{ "find",           whitedb_find },
	{ "free_size",      whitedb_free_size },
	{ "index_s",        whitedb_index_create },
	{ "index_m",        whitedb_index_multi },
	{ "index_drop",     whitedb_index_drop },
	{ "query",          whitedb_query },
	{ "query_t",        whitedb_query_t },
	{ "query_sum_t",    whitedb_query_sum_t },

	{ "query_count",    whitedb_query_count },
	{ "query_count_sum",whitedb_query_count_sum },
	{ "count",          whitedb_count },
	{ "clear",          whitedb_clear },
	{ "print",          whitedb_print },
	{ "first",          whitedb_first },
	{ "next",           whitedb_next },

	{ "import_csv",     whitedb_import_csv },
	{ "export_csv",     whitedb_export_csv },
	{ "import_dump",    whitedb_dump_import },
	{ "export_dump",    whitedb_dump_export },
	
	{ "__gc",		    whitedb_gc },
	{ NULL, NULL }
};

//---------------------------------------------------------
static int register_whitedb_meta(lua_State *l)
{
	luaL_newmetatable( l, WHITEDB_METATABLE );
	lua_pushvalue(l, -1);
	lua_setfield(l, -2, "__index");
	luaL_register(l, NULL, lib_whitedb_meta);
	
	luaL_register(l, WHITEDB_NAME, lib_whitedb);
	lua_pushvalue(l,-1);
	lua_setglobal(l, WHITEDB_NAME );

	return 0;
}

//---------------------------------------------------------
static int register_whitedb_record_meta(lua_State *l)
{
	luaL_newmetatable(l, WHITEDB_RECORD_METATABLE);
	lua_pushvalue(l, -1);
	lua_setfield(l, -2, "__index");

	luaL_register(l, NULL, lib_whitedb_record_meta);

	return 0;
}

//---------------------------------------------------------
WHITE_DB_EXPORT int luaopen_whitedb(lua_State *l)
{
	register_whitedb_record_meta(l);
	register_whitedb_meta(l);
	return 0;
}