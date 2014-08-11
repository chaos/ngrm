package.path = "liblua/?.lua;" .. package.path

local hostlist = require ("hostlist")
local tree = require ("tree")

if pepe.rank == 0 then
    local env = pepe:getenv()
    local v,err = pepe:getenv("ENV")
    if not v then
	print ("getenv(ENV): " .. err .. "\n")
    end
    pepe:unsetenv ("ENV")
    pepe:setenv ("HAVE_PEPE", 1)
    pepe:setenv ("PS1", "${SLURM_JOB_NODELIST} \\\u@\\\h \\\w$ ")

end

local h = hostlist.new (pepe.nodelist)
local mport = 5000 + tonumber (pepe:getenv ("SLURM_JOB_ID")) % 1024;
local eventuri = "epgm://eth0;239.192.1.1:" .. tostring (mport)
local plugins = "modctl,api,barrier,live,log,kvs,job,rexec,resrc"

local right_rank = (pepe.rank + 1) % pepe.nprocs
local right_uri = "tcp://" ..  h[right_rank + 1] .. ":5556"

if pepe.rank == 0 then
    pepe.run ("./cmbd --plugins=hb,sched," .. plugins
                .. " --child-uri='tcp://*:5556'"
                .. " --event-uri='" .. eventuri .. "'"
		.. " --right-uri=" .. right_uri
		.. " --rank=" .. pepe.rank 
		.. " --size=" .. pepe.nprocs
		.. " --logdest=cmbd.log"
		.. " sched:rdl-conf=conf/hype.lua"
		.. " kvs:conf.hb.heartrate=1.5"
		.. " kvs:conf.log.reduction-timeout-msec=100"
		.. " kvs:conf.log.circular-buffer-entries=100000"
		.. " kvs:conf.log.persist-level=debug")
else
    local parent_rank = tree.k_ary_parent (pepe.rank, 2)
    local parent_uri = "tcp://" ..  h[parent_rank + 1] .. ":5556"
    pepe.run ("./cmbd --plugins=" .. plugins
                .. " --child-uri='tcp://*:5556'"
		.. " --parent-uri='" .. parent_uri .. "'"
                .. " --event-uri='" .. eventuri .. "'"
		.. " --right-uri=" .. right_uri
		.. " --rank=" .. pepe.rank
		.. " --size=" .. pepe.nprocs)
end
