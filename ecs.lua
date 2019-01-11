---------------------------------------------------------------------
-- TODO:
-- 优化remove和add component的效率 in c？
-- 优化寻找chunk，代替现在的字符串拼接逻辑
-- add/rm component时，缓存不被update的entity
---------------------------------------------------------------------
-- Debug
local serpent = require "serpent"
function string_split(str, sep)
    local r = {}
    for i in string.gmatch(str, string.format('[^%s]+', sep)) do
        table.insert(r, i)
    end
    return r
end

local printTable = function (t)
	local strList = string_split(serpent.block(t),"\n")
	for _,v in ipairs(strList) do
		print (v)
	end
end

--config

local fixedTimeStep = 33.3333333333333333333333333333


----------------------------------------------------------------------
--[[
	Follow This Order
	
	register_tuple
	register_system inject_data  register_notify
	start_world
]]--
----------------------------------------------------------------------
local ecsc = require "ecsc"
local table = table
local tsort = table.sort

local ecs = {}
local entities = {}
local entity_id = 0
ecs.entities = entities

-- comp_tree use a comp tuple str as key, map to a 'Chunk'
-- a Chunk is comptype str -> array of component data
-- Every Chunk restore a entityid array
local comp_tree = {}

local system_array = {}
local update_array = {}

setmetatable(update_array, {__mode = "v"})

local notify_map = {}

local tupleTypes = {}

local eventOnAddComponent = "OnAddComponent"
local eventOnRemoveComponent = "OnRemoveComponent"
local eventOnNewEntity = "OnNewEntity"
local eventOnDelEntity = "OnDelEntity"

local function clear_data()
	entities = {}
	entity_id = 0
	ecs.entities = entities
	comp_tree = {}
	system_array = {}
	
	update_array = {}
	setmetatable(update_array, {__mode = "v"})

	notify_map = {}

	postUpdateCommandArray = {}
	notify_list_memoize = {}

	ecsc.dispose()
end

-----------------------------------------------------------------------
--- component tree internal functions

local function new_chunk(chunkCode,compTypes)
	assert(comp_tree[chunkCode] == nil)
	
	local newChunk = {}
	for _,typeStr in ipairs(compTypes) do
		newChunk[typeStr] = {}
	end
	newChunk.entityIDs = {}
	newChunk._compTypes = compTypes
	newChunk._nextChunks = {}

	notify_map[newChunk] = {}

	return newChunk
end

local function chunk_tostring(c)
	return table.concat(c._compTypes)
end

local function get_upward_chunk_name(t)
	if #t == 1 then
		return t
	end
			
	local head,rest = t[1],{unpack(t,2)}
	local combine_rest = get_upward_chunk_name(rest)
	local len = #combine_rest
	local result = {}
	result[1] = head
	local len = #combine_rest
	for i=1,len do
		result[i+len+1] = combine_rest[i]
		result[i+1] = head..combine_rest[i]
	end

	return result
end

local function update_tree(chunk,types)
	local upwardChunks = get_upward_chunk_name(types)
	local upChunk = nil
	local next = nil
	local findUpChunk = false
	for i = 1,#upwardChunks do
		upChunk = comp_tree[upwardChunks[i]]
		if upChunk and upChunk ~= chunk then
			next = upChunk._nextChunks
			if next == nil then
				upChunk._nextChunks = setmetatable({},{__mode="v"})
				next = upChunk._nextChunks
			end
			next[#next+1] = chunk
			findUpChunk = true
		end
	end
	return findUpChunk
end

local function create_tuple_chunks()
	local chunk
	for _,v in pairs(tupleTypes) do
		chunk = comp_tree[v.hashCode]
		if (chunk == nil) then
			chunk = new_chunk(v.hashCode,v.comps)
			comp_tree[v.hashCode] = chunk
		end
	end
end


local function create_tree()
	create_tuple_chunks()
	ecsc.init_tree()
	local nextList
	local chunkCt = 0
	for chunkCode,chunk in pairs(comp_tree) do
		chunkCt = chunkCt + 1
		nextList = ecsc.get_next_list(chunkCode)
		chunk._nextChunks = {}
		for i,v in ipairs(nextList) do
			chunk._nextChunks[i] = comp_tree[v]
		end
	end

	printTable(comp_tree)
end

local function update_notify_map()
	local nextChunk
	local nextChunkSet
	for chunk,systemSet in pairs(notify_map) do
		local nextChunkArray = chunk._nextChunks
		--print ("deal with chunk "..chunk_tostring(chunk))
		if nextChunkArray then
			for i=1,#nextChunkArray do
				nextChunk = nextChunkArray[i]
				--print ("--- next chunk "..chunk_tostring(nextChunk))
				if notify_map[nextChunk] then 
					nextSystemSet = notify_map[nextChunk]
					if nextSystemSet then
						for sys,_ in pairs(systemSet) do
							nextSystemSet[sys] = true
							--print ("update_notify_map nextSystemSet[system]"..sys.name)
						end
					end
				end
			end
		end
	end
end

local notify_list_memoize = {}
setmetatable(notify_list_memoize, {__mode = "kv"})
local function get_notify_list(chunk)
	local cached = notify_list_memoize[chunk]
	if cached then
		return cached
	end

	local system
	local notifySet = notify_map[chunk]
	local ret = {}
	if notifySet then
		for iSys = 1,#system_array do
			system = system_array[iSys]
			if notifySet[system] then
				ret[#ret+1] = system
			end
		end
	end
	notify_list_memoize[chunk] = ret
	return ret
end

-- 系统事件按照系统update顺序执行
local function notify_system(chunk,eventName,...)
	local notifyList = get_notify_list(chunk)
	local system
	local eventFunc
	--print ("notify_system "..chunk_tostring(chunk).." "..eventName)
	for i=1,#notifyList do
		system = notifyList[i]
		--print ("---"..system.name)
		eventFunc = system[eventName]
		if eventFunc then
			--print ("called function "..eventName)
			eventFunc(...)
		end
	end
end

local function init_component_data(eid, e, chunk, keyTable, data)
	assert (e.chunk == nil,"Error! Cannot re-init entity data")

	local key
	local compArray
	for i=1,#keyTable do
		key = keyTable[i]
		compArray = chunk[key]
		assert(data[key] ~= nil, "data and group not fit : key = "..key)
		assert(compArray)
		compArray[#compArray+1] = data[key]
	end

	local entityArray = chunk.entityIDs
	entityArray[#entityArray + 1] = eid

	e.chunk = chunk
	e.chunkIndex = #entityArray
end

----------------------------------------------------------------------------------

--==============================--
--desc: create a new entity
--time:2018-04-16 06:09:40
--@tupleType: registered tuple
--@data: all component data
--@return entity id
--==============================--
function ecs.new_entity(tupleType, data)
	local e = {}
	entity_id = entity_id + 1
	entities[entity_id] = e

	local chunk = comp_tree[tupleType.hashCode]
	init_component_data(entity_id, e, chunk, tupleType.comps, data)
	notify_system(chunk,eventOnNewEntity,chunk,e.chunkIndex)
	
	print ("entity created "..entity_id)
	return entity_id
end

function ecs.new_entity_unknown(data)	
	local keyTable = {}
	for k,_ in pairs(data) do
		keyTable[#keyTable+1] = k
	end

	local compGroup = ecs.register_tuple(keyTable)
	return ecs.new_entity(compGroup,data)
end

function ecs.delete_entity(eid)
	local e = assert(entities[eid])
	local index = e.chunkIndex
	local chunk = e.chunk
	local chunkEntities = chunk.entityIDs
	local length = #chunkEntities
	local compArray

	notify_system(chunk, eventOnDelEntity,chunk,index)

	for i=1,#chunk._compTypes do
		compArray = chunk[chunk._compTypes[i]]
		compArray[index] = compArray[length]
		compArray[length] = nil
	end
	local insertedEntity = chunkEntities[length]
	chunkEntities[index] = insertedEntity
	chunkEntities[length] = nil

	entities[insertedEntity].chunkIndex = index

	e.chunk = nil
	e.chunkIndex = nil
	entities[eid] = nil
end

-- for optimizing remove/add behavior
-- pre register a chunk
function ecs.register_tuple(comps)
	local hashCode = ecsc.get_or_create_tuple(comps)
	local tupleType = {}
	tupleType.hashCode = hashCode
	tupleType.comps = comps
	tupleTypes[hashCode] = tupleType
	return tupleType
end

function ecs.data_accessor(eid)
	local ret = {}
	local retmt = {}
	--TODO: 这个upvalue有可能被长期应用造成内存泄露？
	local e = assert(entities[eid])
	retmt.__index = function (t,k)
		local chunk = assert(e.chunk)
		local arr = chunk[k]
		if arr == nil then
			return nil
		else
			return arr[e.chunkIndex]
		end
	end

	return setmetatable(ret,retmt)
end

-- Athena Exclamation
-- Do no use this all the time
function ecs.get_component_data(eid, compName)
	local e = assert(entities[eid])
	local chunk = assert(e.chunk)

	local arr = chunk[compName]
	if arr == nil then
		return nil
	else
		return arr[e.chunkIndex]
	end
end

function ecs.set_component_data(eid, compName, data)
	local e = assert(entities[eid])
	local compArray = assert(e.chunk[compName])
	compArray[e.chunkIndex] = data
end

-- add a component to entity
function ecs.add_component(eid, cType, data)
	local e = assert(entities[eid])

	if e.chunk == nil then
		assert (false,"Awful Design!!! You should not empty a existed entity and add data in it again!")
		local initData = {}
		initData[cType] = data
		local chunk = comp_tree[cType]
		local keyTable = {cType}
		local hashCode = ecsc.get_or_create_tuple(keyTable)
		if chunk == nil then
			chunk = new_chunk(hashCode,keyTable)
		end
		init_component_data(eid,e,chunk,keyTable,initData)
		return
	end

	local chunk = e.chunk

	local srcIndex = e.chunkIndex
	local compTypes = chunk._compTypes
	local newCompTypes = {}
	local alreadyHasType = false
	for i = 1,#compTypes do
		if compTypes[i] == cType then
			alreadyHasType = true
			break
		end
		newCompTypes[#newCompTypes+1] = compTypes[i]
	end

	if alreadyHasType then
		-- maybe should direct set the data
		return 
	end

	newCompTypes[#newCompTypes+1] = cType
	tsort(newCompTypes)
	local newChunkStr = table.concat(newCompTypes)

	local dstChunk = comp_tree[newChunkStr]

	-- if there's upward chunks update tree
	if dstChunk == nil then
		local hashCode = ecsc.get_or_create_tuple(keyTable)
		local newChunk = new_chunk(hashCode,newCompTypes)
		local found = update_tree(newChunk,newCompTypes)

		comp_tree[newChunkStr] = newChunk
		dstChunk = newChunk
		update_notify_map()
	end

	-- same code in remove_component
	if dstChunk then
		-- insert
		local srcEntityIDs = chunk.entityIDs
		local dstEntityIDs = dstChunk.entityIDs
		local key = nil
		local dstCompArray = nil
		local srcCompArray = nil
		local srcLen = #srcEntityIDs
		local dstLen = #dstEntityIDs
		for i=1,#compTypes do
			key = compTypes[i]
			dstCompArray = dstChunk[key]
			srcCompArray = chunk[key]
			dstCompArray[dstLen+1] = srcCompArray[srcIndex]
			srcCompArray[srcIndex] = srcCompArray[srcLen]
			srcCompArray[srcLen] = nil
		end

		dstChunk[cType][dstLen+1] = data
		local inserted = srcEntityIDs[srcLen]
		srcEntityIDs[srcIndex] = inserted
		srcEntityIDs[srcLen] = nil
		entities[inserted].chunkIndex = srcIndex
		 
		dstEntityIDs[dstLen+1] = eid

		e.chunk = dstChunk
		e.chunkIndex = dstLen+1

		notify_system(dstChunk, eventOnAddComponent, cType,dstChunk, dstLen+1)
	end
end

-- remove a component from a entity
-- will not create a new chunk if there isn't any
-- since there is no system for the data tuple
-- THIS COULD EXPENSIVE
function ecs.remove_component(eid, cType)
	local e = assert(entities[eid])
	if e.chunk == nil then
		return false
	end

	local chunk = e.chunk
	if chunk == nil or chunk[cType] == nil then
		return false
	end

	local srcIndex = e.chunkIndex

	local compTypes = chunk._compTypes
	local newCompTypes = {}
	for i = 1,#compTypes do
		if compTypes[i] ~= cType then
			newCompTypes[#newCompTypes+1] = compTypes[i]
		end
	end
	tsort(newCompTypes)
	local newChunkStr = table.concat(newCompTypes)

	local dstChunk = comp_tree[newChunkStr]

	-- if there's upward chunks update tree
	if dstChunk == nil then
		local newChunk = new_chunk(newChunkStr,newCompTypes)
		local found = update_tree(newChunk,newCompTypes)
		if found then
			comp_tree[newChunkStr] = newChunk
			dstChunk = newChunk
		else
			-- no system inject the data
			-- TODO 缓存起来，免得被gc
		end
	end

	if dstChunk then
		-- insert
		local srcEntityIDs = chunk.entityIDs
		local dstEntityIDs = dstChunk.entityIDs
		local key = nil
		local dstCompArray = nil
		local srcCompArray = nil
		local srcLen = #srcEntityIDs
		for i=1,#newCompTypes do
			key = newCompTypes[i]
			dstCompArray = dstChunk[key]
			srcCompArray = chunk[key]
			dstCompArray[#dstCompArray+1] = srcCompArray[srcIndex]
			
			srcCompArray[srcIndex] = srcCompArray[srcLen]
			srcCompArray[srcLen] = nil
		end

		local inserted = srcEntityIDs[srcLen]
		srcEntityIDs[srcIndex] = inserted
		srcEntityIDs[srcLen] = nil

		entities[inserted].chunkIndex = srcIndex

		dstEntityIDs[#dstEntityIDs+1] = eid

		e.chunk = dstChunk
		e.chunkIndex = #dstEntityIDs

		notify_system(chunk, eventOnRemoveComponent, cType, nil)
	end
end

local function new_iterator(chunk)
	local iter = {}
	local iter_mt = {}

	iter.data = chunk
	iter.len = #chunk.entityIDs
	iter.index = 0

	function iter.iter_next(t)
		local index = t.index
		if index == 0 then
			t.index = t.index + 1
			t.data = chunk
			t.entityArray = chunk.entityIDs
			t.len = #chunk.entityIDs
			return true
		else
			if chunk._nextChunks then
				local data = chunk._nextChunks[index]
				if data then
					t.index = t.index + 1
					t.data = data
					t.entityArray = data.entityIDs
					t.len = #data.entityIDs
					return true
				end
				return false
			end
		end
		return false
	end

	function iter.iter_next_direct(t)
		local index = t.index
		if index == 0 then
			t.index = t.index + 1
			t.data = chunk
			t.entityArray = chunk.entityIDs
			t.len = #chunk.entityIDs
			return true
		end
		return false
	end

	return setmetatable(iter,iter_mt)
end

-- start to iterator data
function ecs.iterator(data)
	assert (data)
	local iter = data.iter
	iter.index = 0
	if data._chunk._nextChunks then
		getmetatable(iter).__call = iter.iter_next
	else
		getmetatable(iter).__call = iter.iter_next_direct
	end
	return data.iter
end

-- called in system code
-- suggest use as a upvalue in system code
function ecs.inject_data(t)
	local hashCode = ecsc.get_or_create_tuple(t)
	local chunk = comp_tree[hashCode]
	if (chunk == nil) then
		chunk = new_chunk(hashCode,t)
		comp_tree[hashCode] = chunk
	end
	
	local ret = {}
	ret._chunk = chunk
	ret.iter = new_iterator(chunk)

	return ret
end

function ecs.register_notify(system, systemData)
	local chunk = systemData._chunk
	notify_map[chunk] = {}
	notify_map[chunk][system] = true
	-- insert leading chunks after tree created
	--print ("register_notify  "..system.name.." "..chunk_tostring(chunk))
end

function ecs.register_system()
	local newSystem = {}
	system_array[#system_array+1] = newSystem

	return newSystem
end

--------------------------------------------------------------------------
-- Post Update Commands
--------------------------------------------------------------------------
local postUpdateCommandArray = {}

local post_commands = {}


local function add_post_command(type,content)
	table.insert(postUpdateCommandArray, {type = type, content = content})
end

function post_commands.add_component(eid,cType,data)
	add_post_command("AddComponent",{eid,cType,data})
end

function post_commands.remove_component(eid,cType)
	add_post_command("RemoveComponent",{eid,cType})
end

function post_commands.new_entity(tuple,data)
	add_post_command("NewEntity",{tuple,data})
end

function post_commands.delete_enitity(eid)
	add_post_command("DelEntity",{eid})
end

ecs.post_commands = post_commands

local function execute_all_commands()
	local content

	local i = 1
	local cmd = postUpdateCommandArray[i]
	while cmd do
		content = cmd.content
		
		if cmd.type == "AddComponent" then
			ecs.add_component(content[1], content[2], content[3])
		elseif cmd.type == "RemoveComponent" then
			ecs.remove_component(content[1], content[2])
		elseif cmd.type == "NewEntity" then
			ecs.new_entity(content[1], content[2])
		elseif cmd.type == "DelEntity" then
			ecs.delete_entity(content[1])
		end

		i = i + 1
		cmd = postUpdateCommandArray[i]
	end

	if i > 1 then
		postUpdateCommandArray = {}
	end
end

local timeAccumulator = 0

function ecs.start_world()
	create_tree()
	
	update_notify_map()

	local sys
	for i=1,#system_array do
		sys = system_array[i]
		if sys.update then
			update_array[#update_array+1] = sys.update
		end
	end

	LuaCollision:StartCollisionSystem()

	timeAccumulator = 0
end

function ecs.update_world(dt)
	local sys
	timeAccumulator = timeAccumulator + dt
	while timeAccumulator >= fixedTimeStep do
		for i=1,#update_array do
			update_array[i](fixedTimeStep)
		end
		execute_all_commands()
		timeAccumulator = timeAccumulator - fixedTimeStep
	end
end

function ecs.delete_world()
	LuaCollision:StopCollisionSystem()
	clear_data()
end

return ecs
