
uses "Node"

Hierarchy "default"
{
    Resource
	{ "cluster",
	  name = "hype",
	  tags = {
		 ["max_bw"] = 2449, ["alloc_bw"] = 0
	  },
	  children = {
		 ListOf{
			Node,
			ids = "201-354",
			args = {
			   name = "hype",
			   sockets = {"0-7", "8-15"},
			   tags = {
				  ["max_bw"] = 145,
				  ["alloc_bw"] = 0 }
			}
		 },
	  }
    }
}
