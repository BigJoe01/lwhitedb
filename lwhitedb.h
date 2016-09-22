#if !defined( __lua_whitedb__)
#define __lua_whitedb__

#include <lua.h>

#ifdef _WIN32
	#define WHITE_DB_EXPORT __declspec (dllexport)
#else
	#define WHITE_DB_EXPORT
#endif

#ifdef __cplusplus
	extern "C" {
#endif

WHITE_DB_EXPORT int luaopen_whitedb(lua_State *l);

#ifdef __cplusplus
	}
#endif

#endif