Socket = Resource:subclass ('Socket')

function Socket:initialize (arg)
    local cpuset = require 'cpuset'.new

    assert (tonumber(arg.id),   "Required Socket arg `id' missing")
    assert (type(arg.cpus) == "string", "Required Socket arg `cpus' missing")

    Resource.initialize (self,
        { "socket",
          id = arg.id,
          properties = { cpus = arg.cpus },
		  tags = arg.tags or {}
        }
    )
    --
    -- Add all child cores:
    --
    local id = 0
    local cset = cpuset (arg.cpus)

	local num_cores = 0
	for _ in cset:setbits() do
	   num_cores = num_cores + 1
	end

	local bw_per_core = 0
	if arg.tags ~=nil and arg.tags['max_bw'] ~= nil then
	   bw_per_core = arg.tags.max_bw / num_cores
	end

    for core in cset:setbits() do
        self:add_child (
		   Resource{ "core", id = core, properties = { localid = id }, tags = { ["max_bw"] = bw_per_core, ["alloc_bw"] = 0 }}
        )
        id = id + 1
    end
end

return Socket

-- vi: ts=4 sw=4 expandtab
