local ffih = require 'ffi/ffi_helper'
require 'minibackend'

local whitedb = whitedb

local db = whitedb.attach( 'test', 1024*1024*100 , 0 )
local random = math.random

local test_iterator_count = 10000
local test_record_count = 10000
-- fill table

local src_table = {}
local fill_table = {}
local fill_time  = 0
local fill_start 
local success
local counter1, counter2, counter3
local all_records = test_iterator_count * test_record_count
print('\n')

print('\n')

print( 'Used memory : ' .. collectgarbage('count') .. ' kb ')

print('\n')

print( 'Total records : ' .. ( all_records ) )

print('\n')

fill_start = ffih.tick_ns()
for i = 1, test_record_count do
    src_table[#src_table+1] = { random(1,100), random(1,100), random(1,100), tostring(random(1,100)) , tostring(random(1,100)),tostring(random(1,100))  } 
end
for i = 1, test_record_count do
    fill_table[#fill_table+1] = src_table[i]
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Table fill time ( ' .. fill_time .. ' ns)' )
print('Table ( ' .. (all_records) / fill_time .. ' rec/s)' )
print('\n')

--[[
fill_start = ffih.tick_ns()
for i = 1, test_record_count do
    local row = src_table[i]
    fill_table[#fill_table+1] = { row[1], row[2], row[3] }
    fill_table[#fill_table+1] = src_table[i]
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Table fill time with create( ' .. fill_time .. ' ns)' )
]]

fill_start = ffih.tick_ns()
for i = 1, test_record_count do
    db:record_t( src_table[i] )
end

fill_time = ffih.tick_diff_ns( fill_start )
print('Whitedb fill time ( ' .. fill_time .. ' ns)' )
print('Whitedb ( ' .. (all_records) / fill_time .. ' rec/s)' )
print('\n')
print( 'Used memory : ' .. collectgarbage('count') .. ' kb ')
src_table = nil

--[[db:clear();

fill_start = ffih.tick_ns()
for i = 1, test_record_count do
    db:record_t( src_table[i] )
end

fill_time = ffih.tick_diff_ns( fill_start )
print('Whitedb fill time ( ' .. fill_time .. ' ns)' )
print('Whitedb ( ' .. (all_records) / fill_time .. ' rec/s)' )
print('\n')

]]


success = db:index_s( 1 )
success = db:index_s( 2 )

local query_data = {
    { column = 1, cond = '>', value = 10 },
    { column = 1, cond = '<', value = 50 },
}

local multi_index = { 50, nil,  }
success = db:index_m( 5, query_data )
--db:print()


print('\n')

fill_start = ffih.tick_ns()
for x = 1, test_iterator_count do
counter1 = 0
for i = 1, test_record_count do
    local row = fill_table[i]
    local row0 = row[1]
    if row0 > 10 and row0 < 50 then
        counter1 = counter1 + 1
    end
end
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Table count time 1 field ( ' .. fill_time .. ' ns) count : ' .. counter1 )


counter2 = 0
fill_start = ffih.tick_ns()

for x = 1, test_iterator_count do
	counter2 = db:query_count( query_data )
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Whitedb count time 1 field( ' .. fill_time .. ' ns) count : ' .. counter2 )


print('\n')

fill_start = ffih.tick_ns()
for x = 1, test_iterator_count do
counter1 = 0
for i = 1, test_record_count do
    local row = fill_table[i]
    local row1 = row[1]
    local row2 = row[2]
    if row1 > 10 and row1 < 50 and row2 > 30 and row2 < 60 then
        counter1 = counter1 + 1
    end
end
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Table count time 2 field ( ' .. fill_time .. ' ns) count : ' .. counter1 )

local query_data = {
    { column = 1, cond = '>', value = 10 },
    { column = 1, cond = '<', value = 50 },
    { column = 2, cond = '>', value = 30 },
    { column = 2, cond = '<', value = 60 },
}


counter2 = 0
fill_start = ffih.tick_ns()
for x = 1, test_iterator_count do
    counter2 = db:query_count( query_data )
end
--[[for record in db:query( query_data ) do
    counter2 = counter2 + 1
end]]

fill_time = ffih.tick_diff_ns( fill_start )
print('Whitedb count time 2 field( ' .. fill_time .. ' ns) count : ' .. counter2 )

print('\n')

fill_start = ffih.tick_ns()
for x = 1, test_iterator_count do
counter1 = 0
for i = 1, test_record_count do
    local row = fill_table[i]
    local row1 = row[1]
    local row2 = row[2]
    local row3 = row[3]
    if row1 > 10 and row1 < 50 and row2 > 30 and row2 < 60 and row3 > 60 and row3 < 100  then
        counter1 = counter1 + 1
    end
end
end
fill_time = ffih.tick_diff_ns( fill_start )
print('Table count time 3 field ( ' .. fill_time .. ' ns) count : ' .. counter1 )

local query_data = {
    { column = 1, cond = '>', value = 10 },
    { column = 2, cond = '>', value = 30 },
    { column = 1, cond = '<', value = 50 },
    { column = 2, cond = '<', value = 60 },
    { column = 3, cond = '>', value = 60 },
    { column = 3, cond = '<', value = 100 },

}

fill_start = ffih.tick_ns()
for x = 1, test_iterator_count do
    counter2 = 0
    counter2 = db:query_count( query_data )
end
--[[for record in db:query( query_data ) do
    counter2 = counter2 + 1
end]]

fill_time = ffih.tick_diff_ns( fill_start )
print('Whitedb count time 3 field( ' .. fill_time .. ' ns) count : ' .. counter2 )