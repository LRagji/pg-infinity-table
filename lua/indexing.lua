local type_max_key = KEYS[1]
local type_min_key = KEYS[2]
local max = tonumber(ARGV[1])
local min = tonumber(ARGV[2])
local table_name = ARGV[3]

local max_exists = redis.call("EXISTS",type_max_key);
local min_exists = redis.call("EXISTS",type_min_key);

local max_result=0
local min_result=0

if max_exists == 0 then 
    max_result = redis.call("ZADD",type_max_key,max,table_name)
else
    local existing_max_rank = redis.call("ZRANK",type_max_key,table_name)
    if type(existing_max_rank)=='boolean' and existing_max_rank == false then
        max_result = redis.call("ZADD",type_max_key,max,table_name)
    else
        local existing_max_value = redis.call("ZRANGE",type_max_key,existing_max_rank,existing_max_rank,"WITHSCORES")
        if tonumber(existing_max_value[2]) < max then 
            redis.call("ZREMRANGEBYRANK",type_max_key,existing_max_rank,existing_max_rank)
            max_result = redis.call("ZADD",type_max_key,max,table_name)
        end
    end
end 

if min_exists == 0 then 
    min_result = redis.call("ZADD",type_min_key,min,table_name)
else
    local existing_min_rank = redis.call("ZRANK",type_min_key,table_name)
    if type(existing_min_rank)=='boolean' and existing_min_rank == false then
        min_result = redis.call("ZADD",type_min_key,min,table_name)
    else
        local existing_min_value = redis.call("ZRANGE",type_min_key,existing_min_rank,existing_min_rank,"WITHSCORES")
        if tonumber(existing_min_value[2]) > min then 
            redis.call("ZREMRANGEBYRANK",type_min_key,existing_min_rank,existing_min_rank)
            min_result = redis.call("ZADD",type_min_key,min,table_name)
        end
    end
end 

return {max_result,min_result}

--redis-cli --eval /Users/laukikragji/Documents/Git/Local/partition-pg/lua/indexing.lua Type1Max Type1Min , 100 1 Type1-1-1000