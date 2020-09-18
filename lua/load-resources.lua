local inventory_key = KEYS[1]
local db_id = ARGV[1]
local max_tables = tonumber(ARGV[2])
local rows_per_table = tonumber(ARGV[3])

while(max_tables>0)
do
    redis.call("RPUSH",inventory_key,db_id .. "," .. max_tables ..  "," .. rows_per_table)
    max_tables = max_tables-1
end

--redis-cli --eval /Users/laukikragji/Documents/Git/Local/partition-pg/lua/load-resources.lua Inventory , 1 100000 100