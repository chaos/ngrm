
uses "Node"

Hierarchy "default" 
{
   Resource { "cluster", name = "cab",
              tags = { ["max_bw"] = 10000, ["alloc_bw"] = 0 },
              children = { ListOf{ Node, ids = "1-1232",
                                   args = { 
                                      name = "cab", 
                                      sockets = {"0-7", "8-15"},
                                      tags = { ["max_bw"] = 480, ["alloc_bw"] = 0 }
                                   }
                                 }
              }
   }
}
