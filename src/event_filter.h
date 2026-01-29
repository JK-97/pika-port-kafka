#ifndef EVENT_FILTER_H_
#define EVENT_FILTER_H_

#include <memory>
#include <ostream>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

class EventFilter {
 public:
  struct KeyMatcher {
    enum class Kind { kExact, kPrefix, kRegex };
    Kind kind{Kind::kExact};
    std::string raw;
    std::string text;
    std::regex regex;

    bool Match(std::string_view key) const;
  };

  struct Group {
    std::vector<KeyMatcher> key_fast;
    std::vector<KeyMatcher> key_regex;
    std::vector<std::string> types;
    std::vector<std::string> actions;
  };

  static std::shared_ptr<const EventFilter> Build(const std::vector<std::string>& group_specs,
                                                  const std::vector<std::string>& exclude_specs,
                                                  std::vector<std::string>* warnings);

  bool ShouldSend(std::string_view key, std::string_view type, std::string_view action) const;
  void Dump(std::ostream& out) const;

 private:
  EventFilter(std::vector<Group> groups,
              std::vector<KeyMatcher> exclude_fast,
              std::vector<KeyMatcher> exclude_regex);

  static bool ParseGroupSpec(const std::string& spec, Group* out, std::string* error);
  static void ParseKeyList(const std::string& value,
                           std::vector<KeyMatcher>* out_fast,
                           std::vector<KeyMatcher>* out_regex,
                           std::vector<std::string>* warnings);
  static bool ParseKeyMatcher(const std::string& value, KeyMatcher* out, std::string* error);

  static std::string Trim(std::string_view value);
  static std::vector<std::string> Split(std::string_view value, char delim);
  static std::string ToLower(std::string_view value);
  static bool IsLowerAscii(std::string_view value);
  static bool EqualsIgnoreCase(std::string_view value, std::string_view lower_value);
  static bool IsUnknown(std::string_view value);

  static bool MatchKeys(std::string_view key,
                        const std::vector<KeyMatcher>& fast,
                        const std::vector<KeyMatcher>& regex);
  static bool MatchValues(std::string_view value, const std::vector<std::string>& values, bool allow_unknown);

  std::vector<Group> groups_;
  std::vector<KeyMatcher> exclude_fast_;
  std::vector<KeyMatcher> exclude_regex_;
};

#endif  // EVENT_FILTER_H_
