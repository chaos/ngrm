local URI = require 'RDL.uri'
local serialize = require 'RDL.serialize'

local MemStore = {}
MemStore.__index = MemStore

local function new (args)
    local d = {
        __types = {},
        __resources = {},
        __hierarchy = {},
    }
    return setmetatable (d, MemStore)
end

--
-- https://stackoverflow.com/questions/640642/how-do-you-copy-a-lua-table-by-value
--
local function deepcopy_no_metatable(o, seen)
    seen = seen or {}
    if o == nil then return nil end
    if seen[o] then return seen[o] end

    local no
    if type(o) == 'table' then
        no = {}
        seen[o] = no

        for k, v in next, o, nil do
            no[deepcopy_no_metatable(k, seen)] = deepcopy_no_metatable(v, seen)
        end
        --setmetatable(no, deepcopy(getmetatable(o), seen))
    else -- number, string, boolean, etc
        no = o
    end
    return no
end

function MemStore:addtype (t)
    self.__types [t.name] = deepcopy_no_metatable (t)
end

function MemStore:store (r)
    if not r.uuid then return nil, "no uuid for resource" end
    local new = deepcopy_no_metatable (r)
    if not new.tags then
        new.tags = {}
    end
    new.tags [r.type] = 1
    self.__resources [r.uuid] = new
    return new
end

-- Return a reference to a single resource data table
function MemStore:get (id)
    return self.__resources [id]
end

local function table_empty (t)
    if next(t) == nil then
        return true
    end
    return false
end

-- Unlink child resource [child] from [parent]
local function unlink_child (store, parent, child)
    local name = store:resource_name (child.id)
    local res = store:get (child.id)

    -- remove from parent's children list, remove this hierarchy
    --  from resource hierarchy list
    parent.children[name] = nil
    res.hierarchy [child.hierarchy.name] = nil
    if table_empty (res.hierarchy) then
        store.__resources [res.uuid] = nil
    end

    -- Now unlink all children:
    for n,grandchild in pairs (child.children) do
        unlink_child (store, child, grandchild)
    end
    return true
end

-- Unlink resource ID at uri [arg]
function MemStore:unlink (arg)
    local uri = URI.new (arg)
    if not uri then
        return nil, "bad URI: "..arg
    end

    -- Get a reference to this uri:
    local res = self:get_hierarchy (arg)
    if not res then
        return nil, "Resource "..arg.." not found"
    end

    -- Get a reference to the parent resource:
    local parent = self:get_hierarchy (tostring (uri.parent))
    if not parent then
        return nil, uri.parent .. ": Not found"
    end
    return unlink_child (self, parent, res)
end

function MemStore:tag (id, tag, value)
    local r = self:get (id)
    if not r then return r, "not found" end
    if not value then
        value = true
    end
    r.tags[tag] = value
    return true
end

function MemStore:resource_name (id)
    local r = self:get (id)
    if not r then return nil end
    return r.name .. (r.id or "")
end

function MemStore:ids ()
    local ids = {}
    for id,_ in pairs(self.__resources) do
        table.insert (ids, id)
    end
    return ids
end

-- Link resource id with hierarchy uri
function MemStore:link (resource, hierarchy)
    -- Store this resource if not in DB
    --
    local r = self:get (resource.uuid)
    if not r then
        r = self:store (resource)
    end
    -- Save a reference to [hierarchy] in this resource
    --  by its name
    --
    r.hierarchy [hierarchy.name] = hierarchy.uri
    return r
end

local function ret_error (...)
    return nil, string.format (...)
end

--
-- create a new hierarchy with parent info.name, info.uri
--
function MemStore:hierarchy_create (info, node)

    -- Create new table to contain a hierarchy node:
    --  node = {
    --     id = resource-uuid,
    --     hierarchy = { name = 'name', uri = 'path' }
    --     children = { <list of child nodes > }
    --     parent = link to parent or nil
    --  }
    local n = {
        id = node.resource.uuid,
        hierarchy = {
            name = info.name,
            uri = "/" .. tostring (node.resource)
        },
        children = {}
    }
    -- Prepend the parent uri to this uri
    if info.uri then
        n.hierarchy.uri = info.uri .. n.hierarchy.uri
    end

    -- Setup to append children to this node
    local info = {
        name = info.name,
        uri = n.hierarchy.uri
    }

    -- Add a link to this entry to the resource database
    self:link (node.resource, n.hierarchy)

    -- Add all children recursively
    for _,child in pairs (node.children) do
        local cname = tostring (child.resource)
        local new = self:hierarchy_create (info, child)
        new.parent = n
        n.children [cname] = new
    end
    return n
end

-- Store hierarchy node representation hnode at uri
--  in the current store.
--
-- If uri == "name:/path/to/node" then we try to get
--  'node' as parent of [hnode]. Otherwise, assume
--  [uri] is a new named hierarchy to register and
--  insert the heirarchy in hierarchy[uri].
--
function MemStore:hierarchy_put (uri, hnode)
    if not uri or not hnode then return nil, "invalid args" end
    local uri = URI.new (uri)

    --
    -- If a path to a resource was specified then try to get the
    --  new object's parent. If this fails, error, otherwise
    --  attach the new object to the parent.
    --
    if uri.path then
        local parent = self:hierarchy_get (tostring (uri.parent))
        if not parent then
            return ret_error ("URI: %s: not found", uri)
        end
        return self:hierarchy_create (parent.hierarchy, hnode)
    end

    --
    -- No path, so we store hierarchy at hierarchy [uri.name].
    -- For now, error if a hierarchy already exists for this name.
    --
    if self.__hierarchy [uri.name] then
        return ret_error ("Refusing to replace hierarchy object at '%s'", uri)
    end
    local new = self:hierarchy_create ({name = uri.name}, hnode)
    self.__hierarchy [uri.name] =  new
    return new
end

function MemStore:get_hierarchy (s)
    local  uri, err = URI.new (s)
    if not uri then return nil, err end

    local t = self.__hierarchy [uri.name]
    if not t then return nil, uri.name..": Doesn't exist" end

    if not uri.path then return t end

    local path
    for k,v in uri.path:gmatch("/([^/]+)") do
        if not path then
            -- We are at top level. Since there is no reference to the
            --  top level resource object, check to see if the current
            --  object matches 'k':
            -- Ensure top-level path matches the uri:
            path = k
            if self:resource_name (t.id) ~= path then
                t = nil -- return 'not found' error below
            end
        else
            path = path .. '/' .. k
            t = t.children[k]
        end
        if not t then
            return nil, "path: " ..path.. ": not found"
        end
    end
    return t
end

--
--  Copy a single hierarchy node
--
local function hierarchy_node_copy (n)
    return {
        id = n.id,
        hierarchy = {
            name = n.hierarchy.name,
            uri = n.hierarchy.uri
        },
        children = {},
        parent = n.parent
    }
end

local function hierarchy_validate (t, parent)
    local msg = "hierarchy invalid: "
    local function failure (msg) return nil, "Hierarchy invalid: "..msg end
    if type (t) ~= "table" then
        return failure ("table expected, got "..type(t))
    end
    if not t.id then
        return failure ("resource id missing "..type(t))
    end
    if parent and t.parent ~= parent then
        return failure ("parent reference dangling")
    end
    if type (t.hierarchy) ~= "table" then
        return failure ("hierarchy table missing, got "..type(t.hierarchy))
    end
    if not t.hierarchy.name or not t.hierarchy.uri then
        return failure ("hierarchy table incomplete")
    end
    if type (t.children) ~= "table" then
        return failure ("children table missing, got "..type (t.children))
    end
    for _,v in pairs (t.children) do
        local result, err = hierarchy_validate (v, t)
        if not result then
            return nil, err
        end
    end
    return true
end


local function hierarchy_export (self, arg)
    local h, err = self:get_hierarchy (arg)
    if not h then return nil, err end

    -- First copy hierarchy at this uri:
    local t = deepcopy_no_metatable (h)

    -- Now, traverse up the hierarchy and copy all parents to root
    while t.parent do
        local name = self:resource_name (t.id)
        local parent = hierarchy_node_copy (t.parent)
        parent.children [name] = t
        t.parent = parent
        t = parent
    end

    assert (hierarchy_validate (t))
    return t
end

-- Copy any missing resourcedata from source to dest objects starting with node
local function dup_resources (source, dest, node)
    local r = source:get (node.id)
    if not r then return nil, "Resource "..node.id .." not found" end
    dest.__resources [node.id] = deepcopy_no_metatable (r)

    if not node.children then error ("node "..source:resource_name (node.id) .. " doesn't have children array") end
    for _,child in pairs (node.children) do
        dup_resources (source, dest, child)
    end
    return true
end

-- Merge hierarchy [h2] into hierarchy [h1]
local function hmerge (h1, h2)
    for name, c2 in pairs (h2.children) do
        local c1 = h1.children [name]
        if c1 then
            hmerge (c1, c2)
        else
            h1.children [name] = c2
            c2.parent = h1.children [name]
        end
    end
    return h1
end

-- Merge an exported hierarchy into hierarchy [name] in this store object
function MemStore:merge_exported (name, hierarchy)
    local h = self:get_hierarchy (name)
    if not h then
        self.__hierarchy [name] = hierarchy
    else
        hmerge (self.__hierarchy [name], hierarchy)
    end
    return true
end

function MemStore:merge (uri, name)
    local copy, err = hierarchy_export (self, uri)
    if not copy then return nil, err end

    return self:merge_exported (name, copy)
end

-- Copy hierarchy at uri given by arg into a new memstore object
function MemStore:copy (arg)
    local copy, err = hierarchy_export (self, arg)
    if not copy then return nil, err end

    -- Create a new, empty memstore object:
    local newstore = new ()
    newstore.__hierarchy [copy.hierarchy.name] = copy
    if not dup_resources (self, newstore, copy) then
        return nil, "Failed to duplicate repo at "..arg
    end
    return newstore
end


---
-- ResourceAccumulator
--
-- This is a special RDL store object that contains a source db/store
--  and a destination db/store {src, dst}. It inherets all metamethods
--  from MemStore via the destination store [dst], but has one
--  additional operation "add" which enables adding resources from
--  the source [src] repo one by one.
--
-- See the __index metamethod for how the inheritance is done.
--
local ResourceAccumulator = {}
ResourceAccumulator.__index = function (self, key)
    -- First check for methods in this class:
    if ResourceAccumulator [key] then
        return ResourceAccumulator [key]
    end
    --
    -- Otherwise "forward" method request to underlying MemStore
    --  object [dst]
    --
    local fn = self.dst[key]
    if not fn then return nil end
    return function (...)
        if self == ... then -- method call
            return fn (self.dst, select (2, ...))
        else
            return fn (...)
        end
    end
end

function MemStore:resource_accumulator ()
    local ra = {
        src = self,
        dst = new (),
    }
    return (setmetatable (ra, ResourceAccumulator))
end

function ResourceAccumulator:add (id)
    local r = self.src:get (id)
    if not r then return nil, "Resource "..id.." not found" end
    --
    -- Add each hierarchy containing resource [r] to destination store:
    -- (Don't delay this operation because resources might be deleted
    --  from the source db)
    --
    for name,path in pairs (r.hierarchy) do
        local uri = name..":"..path
        local h = hierarchy_export (self.src, uri)

        local rc, err = self.dst:merge_exported (name, h)
        if not rc then return nil, err end

        local rc, err = dup_resources (self.src, self.dst, h)
        if not rc then return nil, err end
    end
    return true
end

--
--  Create a proxy for the resource object [res] stored in memory store
--   [store]
--
local function resource_proxy_create (store, res)
    local resource, err = store:get (res.id)
    if not resource then return nil, err end

    local counter = 0
    local children = {}
    local sort_by_id = function (a,b)
        local r1 = store:get (res.children[a].id)
        local r2 = store:get (res.children[b].id)
        return r1.id < r2.id
    end

    --  Create table for iterating over named children:
    local function create_child_table ()
        for k,v in pairs (res.children) do
            table.insert (children, k)
        end
    end

    --  Reset counter for iterating children, sorted by id
    --   by default.
    local function reset (self, sortfn)
        if not sortfn then
            sortfn = sort_by_id
        end
        table.sort (children, sortfn)
        counter = 0
    end

    -- Setup: Create child iteration table and reset the counter:
    create_child_table ()
    reset ()

    ---
    -- Iterate over children via built-in counter
    --
    local function next_child()
        counter = counter+1
        if counter <= #children then
            local c = res.children [children [counter]]
            return resource_proxy_create (store, c)
        end
        counter = 0
        return nil
    end

    ---
    -- Unlink child resource with [name] from this hierarchy
    --  and reset counter accordingly
    --
    local function unlink (self, name)
        local child = res.children [name]
        if not child then return nil, "resource not found" end

        -- remove from db:
        unlink_child (store, res, child)

        -- Now need to update local children list:
        for i,v in ipairs (children) do
            if v == name then
                table.remove (children, i)

                -- adjust counter
                if counter >= i then
                    counter = counter - 1
                end
            end
        end
        return true
    end

    local function child_iterator ()
        local i = 0
        local function next_resource (t,index)
            i = i + 1
            local c = res.children [children [i]]
            if not c then return nil end
            return resource_proxy_create (store, c)
        end
        return next_resource, res.children, nil
    end

    ---
    -- Return a table summary of the current resource object
    --
    local function tabulate ()
        return deepcopy_no_metatable (resource)
    end

    ---
    -- Return an aggregation of values for the current resource
    --
    local function aggregate (self)
        return store:aggregate (self.uri)
    end

    ---
    -- Prevent assigning new values to this object:
    --
    local function newindex (self, k, v)
        return nil, "This object cannot be directly indexed"
    end

    ---
    -- Apply a tag to the current resource
    --
    local function tag (self, t, v)
        return store:tag (res.id, t, v)
    end

    ---
    -- Various value accessor functions:
    --
    local function index (self, index)
        if index == "name" then
            return store:resource_name (res.id)
        elseif index == "basename" then
            return resource.name or resource.type
        elseif index == "tags" then
            return deepcopy_no_metatable (resource.tags)
        elseif index == "type" then
            return resource.type
        elseif index == "uri" then
            return res.hierarchy.name..":"..res.hierarchy.uri
        elseif index == "hierarchy_name" then
            return res.hierarchy.name
        elseif index == "path" then
            return res.hierarchy.uri
        elseif index == "id" then
            return resource.id
        elseif index == "uuid" then
            return res.id
        end
    end

    local proxy = {
        children = child_iterator,
        next_child = next_child,
        reset = reset,
        tag = tag,
        tabulate = tabulate,
        aggregate = aggregate,
        unlink = unlink
    }
    return setmetatable (proxy,
        { __index = index,
          __newindex = newindex,
          __tostring = function ()
            return store:resource_name (resource.uuid) end,
         })
end

---
-- Return a resource "proxy" object for URI 'arg' in the store.
--
function MemStore:resource (arg)
    local ref, err = self:get_hierarchy (arg)
    if not ref then return nil, err end
    return resource_proxy_create (self, ref)
end

--
-- Increment the value of table[key] or set to one if currently nil
--
local function increment (table, key)
    table[key] = (table[key] or 0) + 1
    return table[key]
end

local function aggregate_tags (resource, result)
    local r = result or {}
    for k,v in pairs (resource.tags) do
        increment (r, k)
    end
    return r
end

local function aggregate (store, node, result)
    local resource = store:get (node.id)
    if not resource then return nil, "Resource "..node.id.." not found" end
    local result = aggregate_tags (resource, result)
    for _,child in pairs (node.children) do
        aggregate (store, child, result)
    end
    return result
end

function MemStore:aggregate (uri)
    local t,err = self:get_hierarchy (uri)
    if not t then return nil, err end

    return aggregate (self, t)
end

function MemStore:serialize ()
    return serialize (self)
end

local function bless_memstore (t)
    if not t.__resources or not t.__hierarchy or not t.__types then
        return nil, "Doesn't appear to be a memstore table"
    end
    return setmetatable (t, MemStore)
end

local function load_serialized (s, loader)
    -- TODO: Set up deserialization env:
    --
    local f, err = loader (s)
    if not f then return nil, err end

    local rc, t = pcall (f)
    if not rc then return nil, t end

    return setmetatable (t, MemStore)
end

local function load_file (f)
    local f,err = io.open (f, "r")
    if not f then return nil, err end

    return load_serialized (f, loadfile)
end

local function load_string (s)
    return load_serialized (s, loadstring)
end

return {new = new, bless = bless_memstore}

-- vi: ts=4 sw=4 expandtab
