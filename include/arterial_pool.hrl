-record(pool_meta, {
  pool_ref           :: arterial_nif:pool_ref(),
  codec              :: module(),
  size               :: pos_integer(),
  default_timeout_ms :: pos_integer(),
  throttle           :: undefined | {pos_integer(), pos_integer()}
}).
