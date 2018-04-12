---------------------------------------------------------------------
-- TODO:
-- remove/add component in c？
-- better chunk searching
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

----------------------------------------------------------------------
--[[
	Follow This Order
	
	register_tuple
	register_system inject_data  register_notify
	start_world
]]--
----------------------------------------------------------------------
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

local notify_map = {}

local eventOnAddComponent = "OnAddComponent"
local eventOnRemoveComponent = "OnRemoveComponent"
local eventOnNewEntity = "OnNewEntity"
local eventOnDelEntity = "OnDelEntity"

-----------------------------------------------------------------------
--- component tree internal functions

local function new_chunk(chunkStr,compTypes)
	assert(comp_tree[chunkStr] == nil)
	
	local newChunk = {}
	for _,typeStr in ipairs(compTypes) do
		newChunk[typeStr] = {}
	end
	newChunk._entityIDs = {}
	newChunk._compTypes = compTypes
	return newChunk
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

local function create_tree()
	local chunks = {}
	for k,v in pairs(comp_tree) do
		chunks[#chunks+1] = {k,v,#v._compTypes}
	end

	-- sort high layer to low
	tsort( chunks, function (a,b)
		if a[3] > b[3] then
			return true
		end
		return false
	end)
	
	for i=1,#chunks do
		local chunk = chunks[i][2]
		update_tree(chunk, chunk._compTypes)
	end
end


-- 消息通知和injectdata相反
-- 为向上通知
local function update_notify_map()
	local nextChunk
	local nextSystemSet
	for chunk,systemSet in pairs(notify_map) do
		local nextChunkArray = chunk._nextChunks
		if nextChunkArray then
			for i=1,#nextChunkArray do
				nextChunk = nextChunkArray[i]
				nextSystemSet = notify_map[nextChunk]
				if nextSystemSet then
					for system,_ in pairs(systemSet) do
						nextSystemSet[system] = true
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
	for iSys = 1,#system_array do
		system = system_array[iSys]
		if notifySet[system] then
			ret[#ret+1] = system
		end
	end
	notify_list_memoize[chunk] = ret
	return ret
end


-- 系统事件按照系统update顺序执行
local function notify_system(chunk,eventName,ud1,ud2)
	local notifyList = get_notify_list(chunk)
	local system
	local eventFunc
	for i=1,#notifyList do
		system = notifyList[i]
		eventFunc = system[eventName]
		if eventFunc then
			eventFunc(ud1,ud2)
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
		assert(data[key], "data and group not fit")
		assert(compArray)
		compArray[#compArray+1] = data[key]
	end

	local entityArray = chunk._entityIDs
	entityArray[#entityArray + 1] = eid

	e.chunk = chunk
	e.chunkIndex = #entityArray
end

----------------------------------------------------------------------------------

function ecs.new_entity(compGroup, data)
	local e = {}
	entity_id = entity_id + 1
	entities[entity_id] = e

	local chunk = compGroup.chunk
	init_component_data(entity_id, e, chunk, compGroup.comps, data)
	notify_system(chunk,eventOnNewEntity,chunk,e.chunkIndex)
	
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
	local chunkEntities = chunk._entityIDs
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
	tsort(comps)
	local chunkStr = table.concat(comps)
	local chunk = comp_tree[chunkStr]
	if (chunk == nil) then
		chunk = new_chunk(chunkStr,comps)
		comp_tree[chunkStr] = chunk
	end

	local ret = {}
	ret.chunk = chunk
	ret.comps = comps
	
	return ret
end

-- set a group of components to a entity
-- recommand for initialization a entity
-- like this:
-- ecs.set_components(eid,{
-- 	position = {1,2},
-- 	collision = false,
-- 	test = "whalla"
-- })
function ecs.set_components(eid, data)
	local e = assert(entities[eid])
	if e.chunk ~= nil then
		print ("should not turn a component to another!")
		return false
	end

	local keyTable = {}
	for k,v in pairs(data) do
		keyTable[#keyTable+1] = k
	end

	tsort( keyTable )

	local chunkStr = table.concat(keyTable)
	local compChunk = comp_tree[chunkStr]
	if compChunk == nil then
		--no need
		print ("no system for this entity "..chunkStr)
		return
	end

	local key = nil
	local compArray = nil
	for i=1,#keyTable do
		key = keyTable[i]
		compArray = compChunk[key]
		if compArray == nil then
			print ("that's impossible")
			compChunk[key] = {}
			compArray = compChunk[key]
		end
		compArray[#compArray+1] = data[key]
	end

	local entityArray = compChunk._entityIDs
	entityArray[#entityArray + 1] = eid

	e.chunk = compChunk
	e.chunkIndex = #entityArray

	return compChunk
end

-- add a component to entity
function ecs.add_component(eid, cType, data)
	local e = assert(entities[eid])

	if e.chunk == nil then
		assert (false,"Awful Design!!! You should not empty a exist entity and add data in it again!")
		local initData = {}
		initData[cType] = data
		local chunk = comp_tree[cType]
		local keyTable = {cType}
		if chunk == nil then
			chunk = new_chunk(cType,keyTable)
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

	dstChunk = comp_tree[newChunkStr]

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

	-- same code in remove_component
	if dstChunk then
		-- insert
		local srcEntityIDs = chunk._entityIDs
		local dstEntityIDs = dstChunk._entityIDs
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

		notify_system(dstChunk, eventOnAddComponent, dstChunk, dstLen+1)
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

	dstChunk = comp_tree[newChunkStr]

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
		local srcEntityIDs = chunk._entityIDs
		local dstEntityIDs = dstChunk._entityIDs
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
	iter.len = #chunk._entityIDs
	iter.index = 0

	function iter.iter_next(t)
		local index = t.index
		if index == 0 then
			t.index = t.index + 1
			t.data = chunk
			t.entityArray = chunk._entityIDs
			t.len = #chunk._entityIDs
			return true
		else
			if chunk._nextChunks then
				local data = chunk._nextChunks[index]
				if data then
					t.index = t.index + 1
					t.data = data
					t.entityArray = data._entityIDs
					t.len = #data._entityIDs
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
			t.entityArray = chunk._entityIDs
			t.len = #chunk._entityIDs
			return true
		end
		return false
	end

	return setmetatable(iter,iter_mt)
end

-- start to iterator data
function ecs.iterator(data)
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
	tsort(t)
	local ret = {}
	local chunkStr = table.concat(t)
	local chunk = comp_tree[chunkStr]

	if (chunk == nil) then
		chunk = new_chunk(chunkStr,t)
		comp_tree[chunkStr] = chunk
	end
	
	ret._chunk = chunk
	ret.iter = new_iterator(chunk)

	return ret
end

function ecs.register_notify(system, systemData)
	local chunk = systemData._chunk
	notify_map[chunk] = {}
	notify_map[chunk][system] = true
	-- insert leading chunks after tree created
end

function ecs.register_system()
	local newSystem = {}
	system_array[#system_array+1] = newSystem
	return newSystem
end

function ecs.new_world()

end

function ecs.start_world()
	create_tree()
	update_notify_map()
end

function ecs.update_world(dt)
	
end

function ecs.delete_world()

end

return ecs
