#include "json_utils.h"

#include <cstdint>
#include <cstdio>

std::string JsonEscape(const std::string& input) {
  std::string out;
  out.reserve(input.size() + 8);
  for (unsigned char c : input) {
    switch (c) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        if (c < 0x20) {
          char buf[7];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned int>(c));
          out.append(buf);
        } else {
          out.push_back(static_cast<char>(c));
        }
    }
  }
  return out;
}

static const char kBase64Table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string Base64Encode(const std::string& input) {
  std::string out;
  out.reserve(((input.size() + 2) / 3) * 4);
  size_t i = 0;
  while (i + 3 <= input.size()) {
    uint32_t triple = (static_cast<uint8_t>(input[i]) << 16) |
                      (static_cast<uint8_t>(input[i + 1]) << 8) |
                      static_cast<uint8_t>(input[i + 2]);
    out.push_back(kBase64Table[(triple >> 18) & 0x3F]);
    out.push_back(kBase64Table[(triple >> 12) & 0x3F]);
    out.push_back(kBase64Table[(triple >> 6) & 0x3F]);
    out.push_back(kBase64Table[triple & 0x3F]);
    i += 3;
  }

  size_t rem = input.size() - i;
  if (rem == 1) {
    uint32_t triple = (static_cast<uint8_t>(input[i]) << 16);
    out.push_back(kBase64Table[(triple >> 18) & 0x3F]);
    out.push_back(kBase64Table[(triple >> 12) & 0x3F]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    uint32_t triple = (static_cast<uint8_t>(input[i]) << 16) |
                      (static_cast<uint8_t>(input[i + 1]) << 8);
    out.push_back(kBase64Table[(triple >> 18) & 0x3F]);
    out.push_back(kBase64Table[(triple >> 12) & 0x3F]);
    out.push_back(kBase64Table[(triple >> 6) & 0x3F]);
    out.push_back('=');
  }
  return out;
}

bool IsPrintableAscii(const std::string& input) {
  for (unsigned char c : input) {
    if (c < 0x20 || c > 0x7E) {
      return false;
    }
  }
  return true;
}
