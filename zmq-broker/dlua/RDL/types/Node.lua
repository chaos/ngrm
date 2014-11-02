uses "Socket"

Node = Resource:subclass ('Node')
function Node:initialize (arg)
    local basename = arg.name or arg.basename
    assert (basename, "Required Node arg `name' missing")

    local id = arg.id
    assert (arg.sockets, "Required Node arg `sockets' missing")
    assert (type (arg.sockets) == "table",
            "Node argument sockets must be a table of core ids")

    Resource.initialize (self,
        { "node",
          id = id,
          name = basename,
          properties = arg.properties or {},
          tags = arg.tags or {}
        }
    )

	local num_sockets = 0
	for _,_ in pairs (arg.sockets) do
	   num_sockets = num_sockets + 1
	end

	local bw_per_socket = 0
    if arg.tags ~= nil and arg.tags['max_bw'] ~= nil then
	   bw_per_socket = arg.tags.max_bw / num_sockets
    end

    local sockid = 0
    for _,c in pairs (arg.sockets) do
	   self:add_child (Socket{ id = sockid, cpus = c, tags = { ["max_bw"] = bw_per_socket, ["alloc_bw"] = 0 }})
       sockid = sockid + 1
    end
end

return Node

-- vi: ts=4 sw=4 expandtab
