#if !defined( __lua_whitedb__)
#define __lua_whitedb__


#ifdef _WIN32
	#define WHITE_DB_EXPORT __declspec (dllexport)
#endif

#ifdef __cplusplus
	extern "C" {
#endif

WHITE_DB_EXPORT int luaopen_whitedb(lua_State *l);

#ifdef __cplusplus
	}
#endif

#endif // 