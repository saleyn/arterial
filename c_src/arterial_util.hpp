//-----------------------------------------------------------------------------
/// \file   arterial_util.hpp
/// \author Serge Aleynikov
//-----------------------------------------------------------------------------
/// \brief Small, header-only bit-twiddling helpers shared across the
/// lock-free/sharded data structures in this directory (owner_table.hpp,
/// wait_list.hpp, sharded_ttl_map.hpp) -- each of these sizes its
/// underlying storage to a power of 2 so that index wraparound can use a
/// cheap bitmask (`& mask`) instead of a modulo.
//-----------------------------------------------------------------------------
#pragma once

#include <cstddef>

namespace arterial {

/// @brief @return the smallest power of 2 that is >= `n` (returns 1 for
/// `n == 0`).
inline size_t RoundUpPow2(size_t n)
{
  size_t p = 1;
  while (p < n) p <<= 1;
  return p;
}

} // namespace arterial
