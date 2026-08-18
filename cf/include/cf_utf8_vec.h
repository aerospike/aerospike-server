#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

bool cf_utf8_validate_128(const uint8_t* buf, size_t buf_sz);
