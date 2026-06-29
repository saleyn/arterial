#include "arterial_fifo.hpp"

namespace arterial {

// Static member definition for reservation counter
std::atomic<uint64_t> FifoReservationQueue::s_reservation_counter{1000000};

} // namespace arterial