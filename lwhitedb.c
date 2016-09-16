/*
** White db lua module
** Copyright (C) 2016 Joe Oszlanczi joethebig@freemail.hu
** 
**
**/

#include "lua.h"
#include "lauxlib.h"

#include "dbapi.h"
#include "indexapi.h" 

#include "lwhitedb.h"
#include <string.h>
#include <assert.h>

#define  WHITEDB_ITARATOR_METATABLE "whitedb_iterator_meta_table"
#define  WHITEDB_METATABLE          "whitedb_meta_table"
#define  WHITEDB_RECORD_METATABLE   "whitedb_record_meta_table"
#define  WHITEDB_NAME               "whitedb"

#if LUA_VERSION_NUM < 502
	#define luaL_newlib(L,l) (lua_newtable(L), luaL_register(L,NULL,l))
#endif

#define DWhiteDbNameSize 64

static int whitedb_record_metatable_ref;
static int whitedb_metatable_ref;
static int whitedb_record_iterator_ref;



//WG_COND_EQUAL       =
//WG_COND_NOT_EQUAL   !=
//WG_COND_LESSTHAN    <
//WG_COND_GREATER     >
//WG_COND_LTEQUAL     <=
//WG_COND_GTEQUAL     >=


// Field Types
// WG_NULLTYPE 1
// WG_RECORDTYPE 2
// WG_INTTYPE 3
// WG_DOUBLETYPE 4
// WG_STRTYPE 5
// WG_XMLLITERALTYPE 6
// WG_URITYPE 7
// WG_BLOBTYPE 8
// WG_CHARTYPE 9
// WG_FIXPOINTTYPE 10
// WG_DATETYPE 11
// WG_TIMETYPE 12

/** Update contents of one field
*  returns 0 if successful
*  returns -1 if invalid db pointer passed (by recordcheck macro)
*  returns -2 if invalid record passed (by recordcheck macro)
*  returns -3 for fatal index error
*  returns -4 for backlink-related error
*  returns -5 for invalid external data
*  returns -6 for journal error

** Atomic errors

-1 if wrong db pointer
-2 if wrong fieldnr
-10 if new value non-immediate
-11 if old value non-immediate
-12 if cannot fetch old data
-13 if the field has an index
-14 if logging is active
-15 if the field value has been changed from old_data
-16 if the result of the addition does not fit into a smallint
-17 if atomic assignment failed after a large numbe


*/

//---------------------------------------------------------
// iterator modes
// 1 - normal database record iterator
// 2 - normal database parent iterator

typedef struct whitedb_record_iterator
{
	void* whitedb;
	void* record;
	void* parent;
	int   mode;
} whitedb_record_iterator;

//---------------------------------------------------------
typedef struct whitedb_instance
{
	void* whitedb;
	int   mode;
	int   permissions;
	char  name[DWhiteDbNameSize + 1];
} whitedb_instance;

//---------------------------------------------------------
typedef struct whitedb_record
{
	void* whitedb;
	void* record;
	wg_int error;
	//int   size;
} whitedb_record;

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
}

#define INSTANCE_EXIT_BOOL(NAME) if ( !NAME ) { lua_pushboolean(l, 0 ); return 1; }
#define INSTANCE_EXIT_NIL(NAME) if ( !NAME ) { lua_pushnil(l); return 1; }

//---------------------------------------------------------
static int whitedb_record_gc(lua_State *l)
{
	whitedb_record* record = lua_touserdata(l, 1);
	assert(record);
	free(record);
}

//---------------------------------------------------------
static int whitedb_record_to_state(void* whitedb, void* dbrecord, int size,  lua_State *l )
{
	assert(whitedb);
	void* record = dbrecord == NULL ? wg_create_record( whitedb, size) : dbrecord;
	if (record == NULL)
	{
		lua_pushnil(l);
		return 1;
	}

	whitedb_record* pRecord = lua_newuserdata(l, sizeof(whitedb_record));
	pRecord->record = record;
	pRecord->whitedb = whitedb;
	lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_record_metatable_ref );
	lua_setmetatable(l, -2);
	return 1;
}


//---------------------------------------------------------
static void whitedb_add_record_to_state(lua_State *l, void* whitedb, void* record )
{
	whitedb_record* pRecord = lua_newuserdata(l, sizeof(whitedb_record));
	pRecord->record = record;
	pRecord->whitedb = whitedb;
	lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_record_metatable_ref);
	lua_setmetatable(l, -2);
}

//---------------------------------------------------------
static int whitedb_record_create(lua_State *l)
{
	whitedb_record* record = lua_touserdata(l, 1);
	assert(record);
	int               size = 1;
	if (lua_gettop(l) > 1)
		size = lua_tointeger(l, 2);
	return whitedb_record_to_state( record->whitedb, NULL, size, l);
}

//---------------------------------------------------------
static int whitedb_record_create2(lua_State *l)
{
	whitedb_instance* instance = lua_touserdata(l, 1);
	int               size = 1;
	if ( lua_gettop(l) > 1 )
		size = lua_tointeger(l, 2);

	assert(instance);
	assert(instance->whitedb);
	return whitedb_record_to_state(instance->whitedb, NULL, size, l);
}

//---------------------------------------------------------
static int whitedb_record_delete(lua_State *l)
{
	whitedb_record* record = lua_touserdata(l, 1);
	assert(record);

	wg_int iSuccess = wg_delete_record(record->whitedb, record->record);
	lua_pushboolean( l, iSuccess == 0);
	lua_pushinteger(l, iSuccess);
	return 2;
}

//---------------------------------------------------------
static int whitedb_record_iterator_gc(lua_State *l)
{
	whitedb_record_iterator* iterator = (whitedb_record_iterator*)lua_touserdata(l, 1);
	free(iterator);
}


//---------------------------------------------------------
static int whitedb_records_iterator(lua_State *l)
{		
	whitedb_record_iterator* iterator = (whitedb_record_iterator*)lua_touserdata(l, lua_upvalueindex(1));
	void*  next = NULL;

	if (iterator->mode == 1)
	{
		if (iterator->record)
			next = wg_get_next_record(iterator->whitedb, iterator->record);
		else
			next = wg_get_first_record(iterator->whitedb);
	}
	else if (iterator->mode == 2)
	{
		if (iterator->parent)
			next = wg_get_next_parent(iterator->whitedb, iterator->record, iterator->parent);
		else
			next = wg_get_first_parent(iterator->whitedb, iterator->record );
	}
	else
	{
		assert(0);
	}


	if (next)
	{
		if (iterator->mode == 1)
		{
			iterator->record = next;
			whitedb_add_record_to_state(l, iterator->whitedb, next );
		}
		else if (iterator->mode == 2)
		{
			iterator->parent = next;
			whitedb_add_record_to_state(l, iterator->whitedb, next);
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
static int whitedb_records(lua_State *l)
{
	whitedb_instance* instance = lua_touserdata(l, 1);
	assert(instance);

	whitedb_record_iterator* rec_iterator = (whitedb_record_iterator*)lua_newuserdata(l, sizeof(whitedb_record_iterator));
	rec_iterator->mode = 1;
	rec_iterator->parent = NULL;
	rec_iterator->record = NULL;
	rec_iterator->whitedb = instance->whitedb;

	lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_record_iterator_ref);
	lua_setmetatable(l, -2);
	lua_pushcclosure(l, whitedb_records_iterator, 1);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_field_size(lua_State *l)
{
	whitedb_record* record = lua_touserdata(l, 1);
	assert(record);
	wg_int size = wg_get_record_len(record->whitedb, record->record);
	if (size < 0)
		lua_pushnil(l);
	else
		lua_pushinteger(l, size);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_parents(lua_State *l)
{
	whitedb_record* record = lua_touserdata(l, 1);
	assert(record);

	whitedb_record_iterator* rec_iterator = (whitedb_record_iterator*)lua_newuserdata(l, sizeof(whitedb_record_iterator));
	rec_iterator->mode = 2;
	rec_iterator->parent = NULL;
	rec_iterator->record = record->record;
	rec_iterator->whitedb = record->whitedb;

	lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_record_iterator_ref);
	lua_setmetatable(l, -2);
	lua_pushcclosure(l, whitedb_records_iterator, 1);
	return 1;
}

//---------------------------------------------------------
static int whitedb_record_delete2(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = lua_touserdata(l, 1);
	assert(instance);
	whitedb_record* record = lua_touserdata(l, 2);
	assert(record);

	wg_int iSuccess = wg_delete_record(record->whitedb, record->record);
	lua_pushboolean(l, iSuccess == 0);
	lua_pushinteger(l, iSuccess);
	return 2;
}

//---------------------------------------------------------
static int whitedb_read_start(lua_State *l)
{
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	lua_pushboolean( l, wg_start_read( instance->whitedb) );
	return 1;
}

//---------------------------------------------------------
static int whitedb_read_end(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	lua_pushinteger(l, wg_end_read(instance->whitedb, lua_tointeger(l,2) ));
	return 1;
}

//---------------------------------------------------------
static int whitedb_write_start(lua_State *l)
{
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
		lua_pushinteger(l, wg_start_write(instance->whitedb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_write_end(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
		lua_pushinteger(l, wg_end_write(instance->whitedb, lua_tointeger(l, 2)));
	return 1;
}
//---------------------------------------------------------
static int whitedb_log_start(lua_State *l) {
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
		lua_pushinteger(l, wg_start_logging(instance->whitedb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_log_stop(lua_State *l) {
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
		lua_pushinteger(l, wg_stop_logging( instance->whitedb ));
	return 1;
}


//---------------------------------------------------------
static int whitedb_dump(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
		lua_pushinteger(l, wg_dump( instance->whitedb, lua_tostring(l,2) ));
	return 1;
}

//---------------------------------------------------------
static int whitedb_dump_import(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	if (lua_toboolean(l, -1)) {
		lua_pushinteger(l, wg_import_dump(instance->whitedb, lua_tostring(l, 1)) );
	}
	else
	{
		lua_pushinteger(l, wg_import_db_csv(instance->whitedb, lua_tostring(l, 1) ));
	}
	return 1;
}

//---------------------------------------------------------
static int whitedb_size(lua_State *l) {
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	lua_pushnumber(l, wg_database_size(instance->whitedb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_free_size(lua_State *l) {
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	lua_pushnumber(l, wg_database_freesize(instance->whitedb));
	return 1;
}

//---------------------------------------------------------
static int whitedb_log_replay(lua_State *l) {
	assert(lua_gettop(l) > 1);
	whitedb_instance* instance = check_instance(l, 1);
	INSTANCE_EXIT_NIL(instance)
	lua_pushinteger(l, wg_replay_log(instance->whitedb, lua_tostring(l,2) ));
	return 1;
}


//---------------------------------------------------------
static int whitedb_gc(lua_State *l)
{
	whitedb_instance* instance = lua_touserdata(l, 1);
	assert(instance);
	assert(instance->whitedb);

	if (instance)
	{
		if ( instance->mode == 3 ) // existing
			wg_detach_database(instance->whitedb);
		if (instance->mode == 2) // local db
			wg_delete_local_database(instance->whitedb);
		else 
			wg_delete_database(instance->whitedb);
	}
	free( instance );
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

	int param_size = lua_gettop(l);

	const char* name = param_size > 0 ? lua_tostring (l, 1) : NULL;
	int   size       = param_size > 1 ? lua_tointeger(l, 2) : 0;
	int   mode       = param_size > 2 ? lua_tointeger(l, 3) : 0;
	int	  permission = param_size > 3 ? lua_tointeger(l, 4) : 0;
	void* db = NULL;
	if (strlen(name) == 0)
	{
		luaL_error(l, "Invalid database name");
		return 0;
	}

	if (mode == 2)
	{
		db = wg_attach_local_database(size);
	}
	else if (mode == 1)
	{
		if (permission != 0)
			db = wg_attach_logged_database_mode(name, size, mode);
		else
			db = wg_attach_logged_database(name, size);
	}
	else if (mode == 3)
		db = wg_attach_existing_database(name);
	else
	{
		if (mode != 0)
			db = wg_attach_database_mode(name, size, mode);
		else
			db = wg_attach_database(name, size);
	}

	if (db)
	{
		whitedb_instance* instance = (whitedb_instance*)lua_newuserdata(l, sizeof(whitedb_instance));
		instance->mode = mode;
		instance->permissions = permission;
		instance->whitedb = db;
		strcpy_s(instance->name, DWhiteDbNameSize, name);
		lua_rawgeti(l, LUA_REGISTRYINDEX, whitedb_metatable_ref);
		lua_setmetatable(l, -2);
	}
	else
		lua_pushnil(l);

	return 1;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_set(lua_State *l)
{
	assert(lua_gettop(l) > 2);
	whitedb_record* record = lua_touserdata(l, 1);
	int             iIndex = lua_tointeger(l, 2);
	int             iType = lua_type(l, 3);
	assert(record);
	wg_int          iData = 0;
	wg_int          iLen = wg_get_record_len(record->whitedb, record->record);
	wg_int          iResult = 0;

	if (iIndex >= iLen)
	{
#ifdef _DEBUG
		luaL_error(l, "Invalid field index %d", iIndex);
#else
		lua_pushboolean(l, 0);
		return 1;
#endif
	}

		switch (iType)
		{
			case LUA_TBOOLEAN: 
			{
				iData = wg_encode_int(record->whitedb, lua_toboolean(l, iIndex));
				break;
			}

			case LUA_TSTRING:
			{
				const char* pStr = lua_tostring(l, iIndex);
				iData = wg_encode_str(record->whitedb,  pStr , NULL);
				break;
			}

			case LUA_TNUMBER:
			{
				double dValue = lua_tonumber(l, iIndex);
				iData = wg_encode_double(record->whitedb, dValue);
				break;
			}

			case LUA_TNIL:
			default:
			{
				iData = wg_encode_null(record->whitedb, 0);
				break;
			}
		}
	iResult = wg_set_field(record->whitedb, record->record, iIndex, iData);
	lua_pushboolean(l, iResult == 0);
	lua_pushinteger(l, iResult);
	return 2;
}

//-------------------------------------------------------------------------
static int whitedb_record_new(lua_State *l)
{
	assert( lua_gettop(l) > 2 );
	whitedb_record* record = lua_touserdata(l, 1);
	wg_int          index  = lua_tointeger(l, 2);
	int             size   = lua_tointeger(l, 3);

	assert(record);
	assert(record->whitedb);
	
	void* pRecord = wg_create_record( record->whitedb, size);
	wg_int newrec = wg_encode_record( record->whitedb, pRecord);
	wg_int field  = wg_set_field(record->whitedb, record->record, index, newrec );

	lua_pushboolean(l, field == 0 ? 1 : 0);
	return whitedb_record_to_state(record->whitedb, pRecord, size, l );
}

//-------------------------------------------------------------------------
static int whitedb_record_field_new(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* record = lua_touserdata(l, 1);
	int             index  = lua_tointeger(l, 2);	
	wg_int iResult = wg_set_new_field(record->whitedb, record->record, index, 0);
	lua_pushboolean(l, iResult > 0);
	return 1;
}


//-------------------------------------------------------------------------
static int whitedb_record_print(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* record = lua_touserdata(l, 1);
	wg_print_record(record->whitedb, record->record);
	return 0;
}

//-------------------------------------------------------------------------
static int whitedb_record_field_get(lua_State *l)
{
	assert(lua_gettop(l) > 1);
	whitedb_record* record = lua_touserdata(l, 1);
	int             iIndex = lua_tointeger(l, 2);

	assert(record);
	wg_int          iData = 0;
	wg_int          iLen = wg_get_record_len(record->whitedb, record->record);
	wg_int          iResult = 0;

	if (iIndex >= iLen)
	{
		lua_pushboolean(l, 0);
		return 1;
	}
	
	wg_int iFieldType = wg_get_field_type(record->whitedb, record->record, iIndex);
	wg_int pField = wg_get_field(record->whitedb, record->record, iIndex);

	if (!pField)
	{
#ifdef _DEBUG
		luaL_error(l, "Invalid field value!");
#else
		lua_pushnil(l);
		return 1;
#endif
	}

	switch (iFieldType)
	{
		case WG_INTTYPE:
		{
			int bValue = wg_decode_int(record->whitedb, pField);
			lua_pushboolean(l, bValue);
			break;
		}
		case WG_DOUBLETYPE:
		{
			double dValue = wg_decode_double(record->whitedb, pField);
			lua_pushnumber(l, dValue );
			break;
		}

		case WG_STRTYPE:
		{
			char* sValue = wg_decode_str(record->whitedb, pField);
			lua_pushstring(l, sValue);
			break;
		}
		case WG_NULLTYPE:
		case WG_RECORDTYPE:
		default:
			lua_pushnil(l);
		break;
	}

	return 1;
}

//---------------------------------------------------------
static const struct luaL_Reg lib_whitedb_record_iterator_meta[] =
{
	{ "__gc",		whitedb_record_iterator_gc },
	{ NULL, NULL }
};

//---------------------------------------------------------
static const struct luaL_Reg lib_whitedb_record_meta[] =
{
	{ "create",		whitedb_record_create },
	{ "delete",		whitedb_record_delete },
	{ "parents",	whitedb_record_parents },
	{ "size",    	whitedb_record_field_size },
	{ "set",        whitedb_record_field_set },
	{ "get",        whitedb_record_field_get },
	{ "add_field",  whitedb_record_field_new },
	{ "add_rec",    whitedb_record_new },
	{ "print",      whitedb_record_print },
	{ "__gc",		whitedb_record_gc },
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
	{ "create",			whitedb_record_create2 },
	{ "delete",			whitedb_record_delete2 },
	{ "records",		whitedb_records },
	{ "read_start",     whitedb_read_start },
	{ "read_end",       whitedb_read_end   },
	{ "write_start",    whitedb_write_start },
	{ "write_end",      whitedb_write_end },
	{ "log_start",      whitedb_log_start },
	{ "log_stop",       whitedb_log_stop},
	{ "log_replay",     whitedb_log_replay },
	{ "size",           whitedb_size },
	{ "free_size",      whitedb_free_size },
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

	luaL_getmetatable(l, WHITEDB_METATABLE);
	whitedb_metatable_ref = luaL_ref(l, LUA_REGISTRYINDEX);
}

//---------------------------------------------------------
static int register_whitedb_record_meta(lua_State *l)
{
	luaL_newmetatable(l, WHITEDB_RECORD_METATABLE);
	lua_pushvalue(l, -1);
	lua_setfield(l, -2, "__index");

	luaL_register(l, NULL, lib_whitedb_record_meta);

	luaL_getmetatable(l, WHITEDB_RECORD_METATABLE);
	whitedb_record_metatable_ref = luaL_ref(l, LUA_REGISTRYINDEX);
}

//---------------------------------------------------------
static int register_whitedb_record_iterator_meta(lua_State *l)
{
	luaL_newmetatable(l, WHITEDB_ITARATOR_METATABLE);
	luaL_register(l, NULL, lib_whitedb_record_iterator_meta);

	luaL_getmetatable(l, WHITEDB_ITARATOR_METATABLE);
	whitedb_record_iterator_ref = luaL_ref(l, LUA_REGISTRYINDEX);
}

//---------------------------------------------------------
WHITE_DB_EXPORT int luaopen_whitedb(lua_State *l)
{
	register_whitedb_record_iterator_meta(l);
	register_whitedb_record_meta(l);
	register_whitedb_meta(l);
	return 1;
}