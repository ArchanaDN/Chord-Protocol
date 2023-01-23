{application, chord,
 [{description, "Chord by Amogh Mannekote and Gloria Katuka"},
  {vsn, "1.0.0"},
  {registered, []},
  {mod, {main, []}},
  {applications,
   [kernel,
    stdlib
   ]},
  {env,[
    
  ]},
  {modules, [main, chord_api, chord_sup, chord_node, util]},
  {licenses, ["Apache 2.0"]},
  {links, []},
  {mod, [main]}
 ]}.
