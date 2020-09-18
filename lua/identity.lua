--redis-cli --eval /Users/laukikragji/Documents/Git/Local/partition-pg/lua/identity.lua T1 Inventory ,  50
local requested_type_key = KEYS[1]
local inventory_key = KEYS[2]
local requested_range = tonumber(ARGV[1])
local result = {}

local parseResourceDefinition = function (inputstr)
    local sep=","
    local t={}
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
            table.insert(t, tonumber(str))
    end
    return t
end

local loadNextResource =  function (type_key)
    redis.call("DEL",type_key)
    if redis.call("EXISTS",inventory_key) == 0 then return {-1,"No resources available"} end
    local next_resource = redis.call("LPOP",inventory_key)
    local new_resource_def = parseResourceDefinition(next_resource)
    redis.call("HMSET",type_key, "DBID", new_resource_def[1], "TBID",new_resource_def[2], "ROWS", new_resource_def[3], "OFF", "1")
    return {0}
end

if redis.call("EXISTS",requested_type_key) == 0 then
    local status = loadNextResource(requested_type_key)
    if(status[1] ~= 0) then return status end
end

local db_id = tonumber(redis.call("HGET",requested_type_key,"DBID"))
local tb_id = tonumber(redis.call("HGET",requested_type_key,"TBID"))
local max_rows = tonumber(redis.call("HGET",requested_type_key,"ROWS"))
local offset = tonumber(redis.call("HGET",requested_type_key,"OFF"))

while(requested_range > 0)
do
    local capacity = (max_rows - offset) + 1
    if capacity > requested_range then
        if table.getn(result) == 0 then table.insert(result,{requested_range,db_id,tb_id,offset,"START1"}) end --START1
        offset = offset + requested_range
        redis.call("HMSET",requested_type_key,"OFF",offset)
        requested_range = 0
        table.insert(result,{1,db_id,tb_id,offset-1,"END1"}) --END1
    else
        table.insert(result,{requested_range,db_id,tb_id,offset,"START2"}) --START2
        requested_range = requested_range - capacity
        if requested_range <= 0 then table.insert(result,{1,db_id,tb_id,max_rows,"END2"}) end --END2
        offset = 1
        local status = loadNextResource(requested_type_key)
        if(status[1] ~= 0) then 
            if table.getn(result) == 0 then return status end
            table.insert(result, 1, -2)-- Partial Success
            return result
        end
        db_id = tonumber(redis.call("HGET",requested_type_key,"DBID"))
        tb_id = tonumber(redis.call("HGET",requested_type_key,"TBID"))
        max_rows = tonumber(redis.call("HGET",requested_type_key,"ROWS"))
        offset = tonumber(redis.call("HGET",requested_type_key,"OFF"))
    end
end

table.insert(result,1,0)
return result
--Retuns
--[0]: -1 = No more space or ERROR; -2 Partial Sucess; 0 = Sucess;
--[N][0]: Seq Counter
--[N][1]: DB ID
--[N][2]: Table ID
--[N][3]: Row ID