#ifndef JSON_UTILS_H_
#define JSON_UTILS_H_

#include <string>
#include <vector>

std::string JsonEscape(const std::string& input);
std::string Base64Encode(const std::string& input);
bool IsPrintableAscii(const std::string& input);

#endif  // JSON_UTILS_H_
