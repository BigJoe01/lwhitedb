print('Whitedb test begin')
print( '--------------------------------')
require 'minibackend'
local whitedb = whitedb

-- check whitedb
	if whitedb == nil then print( 'Whitedb is nil') return end
-- check whitedb attach
	if whitedb.attach == nil then print( 'Whitedb attach is nil') return end

local db = whitedb.attach( 'test', 1024*1024, 0 )

-- new whitedb
	if db == nil then print( 'Whitedb new db is nil') return end

-- new record create

local rec1count = 4
local rec2count = 4

local rec1 = db:record( rec1count  )
local rec2 = db:record( rec2count  )
local rec3 = db:record( rec2count  )

print( ' record 1 created : ' .. rec1count )
print( ' record 2 created : ' .. rec2count )

-- new whitedb record
	if rec1 == nil then print( 'Whitedb record 1 is empty') return end
	if rec2 == nil then print( 'Whitedb record 2 is empty') return end
	if rec3 == nil then print( 'Whitedb record 3 is empty') return end

print( ' Set record 1 fields')
print( '--------------------------------')
rec1:set( 1, 'x1')
rec1:set( 2, 1 )
rec1:set( 3, 3 )
rec1:set( 4, 1 )


print( ' Set record 2 fields')
print( '--------------------------------')
rec2:set( 1, 'x2')
rec2:set( 2, 2 )
rec2:set( 3, 3 )
rec2:set( 4, 2 )

print( ' Set record 3 fields')
print( '--------------------------------')
rec3:set( 1, 'x3')
rec3:set( 2, 2 )
rec3:set( 3, 3 )
rec3:set( 4, '3' )



local rec1_size = rec1:size()
local rec2_size = rec2:size()
local rec3_size = rec3:size()

-- print record size
print( ' record 1 size : ' .. rec1_size )
print( ' record 2 size : ' .. rec2_size )
print( ' record 3 size : ' .. rec3_size )

-- free database memory
print ( 'database size : '    .. db:size() )
print ( 'database free size ' .. db:free_size() )

print( 'add record')
local new_rec = rec2:record( 2, 2 )

print( 'field type ' .. rec2:type( 2 ) )

print( 'set record')
new_rec:set( 1, 'asdsad' )

-- iterate over records
print('Iterate records' )
print( '--------------------------------')
print('\n')
local count = 0
for rec in db:records() do
	count = count + 1
	print (' iterate : ' .. count )
	rec:print()
    print('\n')
end

print( 'Find x1')
print( '--------------------------------')
local frec = db:find_one( 0, '=', 'x1' )

print( 'Found record : ' .. tostring(frec) )
if frec ~= nil then
    frec:print()
end
print('\n')
print( 'Find 3')
print( '--------------------------------')
for rec in db:find( 2, '=', 3) do
    rec:print()
    print('\n')
end

print('\n')
print( 'Create multi index')
print( '--------------------------------')

local multi = { [1] = {}, [2] = 2 }
local success = db:index_m( 1, multi )
print(' Success : ' .. tostring( success ) )

print('\n')
print( 'Create single index on 1 field')
print( '--------------------------------')

success = db:index_s( 2 )
print(' Success : ' .. tostring( success ) )

print('\n')
print( 'Create single index on 0 field')
print( '--------------------------------')

success = db:index_s( 1 )
print(' Success : ' .. tostring( success ) )

print('\n')
print( 'Query sum')
print( '--------------------------------')

local query_data_sum = {
    { column = 3, cond = '>', value = 1 },
    { column = 3, cond = '<=', value = 4 },
}
local qcount, qsum = db:query_count_sum( query_data_sum, 3 )
print(' count : ' .. qcount .. ' sum : ' .. qsum .. ' \n')


print('\n')
print( 'Query')
print( '--------------------------------')

local query_data = {
    { column = 3, cond = '>', value = 1 },
    { column = 3, cond = '<=', value = 4 },

}

for rec in db:query( query_data ) do
    print('\n')
    rec:print()
end

print('\n')
print( 'Query table')
print( '--------------------------------')
local qtable = db:query_t( query_data )
for i = 1, #qtable do
    local qt = qtable[i]
    print('\n')
    qt:print()
end

print('\n')


print('\n')
print( 'Query "3"')
print( '--------------------------------')
db:print()
local query_data = {
    { column = 4, cond = '=', value = '3' },
}
print('Data\n')
local qtable2 = db:query_t( query_data )
for i = 1, #qtable2 do
    local qt = qtable2[i]
    print('\n')
    qt:print()
end

print('\n')

print( 'Set table')
print( '--------------------------------')
rec3:print()

local rec_table = { 'xx', 67, 45, 5 }
for i = 1,10 do
    rec3:set_t( rec_table )
    rec3:print()
end

print('\n')
print( 'Get table')
print( '--------------------------------')
for i= 1, 10 do
    rec3:get_t( rec_table )
    rec3:print()
end

for k,v in pairs(rec_table) do

    print(v .. '\n')

end

print('\n')
print( 'Print db')
print( '--------------------------------')
db:print()



print('\n')
print( 'Export csv')
print( '--------------------------------')
db:export_csv( 'c:\\temp\\db.csv' )

print('\n')

print('Whitedb test end')