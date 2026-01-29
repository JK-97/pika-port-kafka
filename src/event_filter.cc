#include "event_filter.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace {

bool HasRegexMeta(std::string_view value) {
  for (char ch : value) {
    switch (ch) {
      case '.':
      case '+':
      case '?':
      case '^':
      case '$':
      case '(':
      case ')':
      case '[':
      case ']':
      case '{':
      case '}':
      case '|':
      case '\\':
      case '*':
        return true;
      default:
        break;
    }
  }
  return false;
}

std::string JoinList(const std::vector<std::string>& values, const std::string& empty_value) {
  if (values.empty()) {
    return empty_value;
  }
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << ",";
    }
    out << values[i];
  }
  return out.str();
}

std::string JoinMatchers(const std::vector<EventFilter::KeyMatcher>& fast,
                         const std::vector<EventFilter::KeyMatcher>& regex,
                         const std::string& empty_value) {
  if (fast.empty() && regex.empty()) {
    return empty_value;
  }
  std::ostringstream out;
  bool first = true;
  auto append = [&](const EventFilter::KeyMatcher& matcher) {
    if (!first) {
      out << ",";
    }
    first = false;
    out << matcher.raw;
  };
  for (const auto& matcher : fast) {
    append(matcher);
  }
  for (const auto& matcher : regex) {
    append(matcher);
  }
  return out.str();
}

}  // namespace

bool EventFilter::KeyMatcher::Match(std::string_view key) const {
  switch (kind) {
    case Kind::kExact:
      return key.size() == text.size() && std::equal(key.begin(), key.end(), text.begin());
    case Kind::kPrefix:
      return key.size() >= text.size() && key.compare(0, text.size(), text) == 0;
    case Kind::kRegex:
      return std::regex_search(key.begin(), key.end(), regex);
    default:
      return false;
  }
}

EventFilter::EventFilter(std::vector<Group> groups,
                         std::vector<KeyMatcher> exclude_fast,
                         std::vector<KeyMatcher> exclude_regex)
    : groups_(std::move(groups)),
      exclude_fast_(std::move(exclude_fast)),
      exclude_regex_(std::move(exclude_regex)) {}

std::shared_ptr<const EventFilter> EventFilter::Build(const std::vector<std::string>& group_specs,
                                                      const std::vector<std::string>& exclude_specs,
                                                      std::vector<std::string>* warnings) {
  std::vector<Group> groups;
  std::vector<KeyMatcher> exclude_fast;
  std::vector<KeyMatcher> exclude_regex;

  for (const auto& spec : group_specs) {
    Group group;
    std::string warning;
    if (!ParseGroupSpec(spec, &group, &warning)) {
      if (warnings && !warning.empty()) {
        warnings->push_back("Filter group skipped: " + warning);
      }
      continue;
    }
    if (warnings && !warning.empty()) {
      warnings->push_back("Filter group notice: " + warning);
    }
    groups.push_back(std::move(group));
  }

  for (const auto& spec : exclude_specs) {
    ParseKeyList(spec, &exclude_fast, &exclude_regex, warnings);
  }

  if (groups.empty() && exclude_fast.empty() && exclude_regex.empty()) {
    return nullptr;
  }
  return std::shared_ptr<const EventFilter>(
      new EventFilter(std::move(groups), std::move(exclude_fast), std::move(exclude_regex)));
}

bool EventFilter::ShouldSend(std::string_view key, std::string_view type, std::string_view action) const {
  if (MatchKeys(key, exclude_fast_, exclude_regex_)) {
    return false;
  }
  if (groups_.empty()) {
    return true;
  }

  for (const auto& group : groups_) {
    bool key_ok = group.key_fast.empty() && group.key_regex.empty()
                      ? true
                      : MatchKeys(key, group.key_fast, group.key_regex);
    bool type_ok = MatchValues(type, group.types, true);
    bool action_ok = MatchValues(action, group.actions, false);
    if (key_ok && type_ok && action_ok) {
      return true;
    }
  }
  return false;
}

void EventFilter::Dump(std::ostream& out) const {
  out << "Event_filter_groups:" << groups_.size() << std::endl;
  for (size_t i = 0; i < groups_.size(); ++i) {
    const auto& group = groups_[i];
    out << "Event_filter_group[" << i << "].key:" << JoinMatchers(group.key_fast, group.key_regex, "any")
        << std::endl;
    out << "Event_filter_group[" << i << "].type:" << JoinList(group.types, "any") << std::endl;
    out << "Event_filter_group[" << i << "].action:" << JoinList(group.actions, "any") << std::endl;
  }
  out << "Event_filter_exclude:" << JoinMatchers(exclude_fast_, exclude_regex_, "none") << std::endl;
}

bool EventFilter::ParseGroupSpec(const std::string& spec, Group* out, std::string* error) {
  if (!out) {
    return false;
  }
  Group group;
  std::string trimmed = Trim(spec);
  if (trimmed.empty()) {
    if (error) {
      *error = "empty group spec";
    }
    return false;
  }

  bool has_value = false;
  std::vector<std::string> warnings;
  for (const auto& token : Split(trimmed, ';')) {
    std::string part = Trim(token);
    if (part.empty()) {
      continue;
    }
    size_t pos = part.find('=');
    if (pos == std::string::npos) {
      warnings.push_back("missing '=' in '" + part + "'");
      continue;
    }
    std::string field = ToLower(Trim(part.substr(0, pos)));
    std::string value = Trim(part.substr(pos + 1));
    if (field == "key") {
      size_t before = group.key_fast.size() + group.key_regex.size();
      ParseKeyList(value, &group.key_fast, &group.key_regex, &warnings);
      size_t after = group.key_fast.size() + group.key_regex.size();
      has_value = has_value || (after > before);
    } else if (field == "type") {
      auto items = Split(value, ',');
      size_t added = 0;
      for (const auto& item : items) {
        std::string v = Trim(item);
        if (v.empty()) {
          continue;
        }
        group.types.push_back(ToLower(v));
        ++added;
      }
      if (added == 0) {
        warnings.push_back("empty type list");
      } else {
        has_value = true;
      }
    } else if (field == "action") {
      auto items = Split(value, ',');
      size_t added = 0;
      for (const auto& item : items) {
        std::string v = Trim(item);
        if (v.empty()) {
          continue;
        }
        group.actions.push_back(ToLower(v));
        ++added;
      }
      if (added == 0) {
        warnings.push_back("empty action list");
      } else {
        has_value = true;
      }
    } else {
      warnings.push_back("unknown field '" + field + "'");
    }
  }

  if (!has_value) {
    if (error) {
      *error = "no valid conditions in '" + trimmed + "'";
    }
    return false;
  }
  if (error && !warnings.empty()) {
    std::ostringstream out_msg;
    for (size_t i = 0; i < warnings.size(); ++i) {
      if (i > 0) {
        out_msg << "; ";
      }
      out_msg << warnings[i];
    }
    *error = out_msg.str();
  }
  *out = std::move(group);
  return true;
}

void EventFilter::ParseKeyList(const std::string& value,
                               std::vector<KeyMatcher>* out_fast,
                               std::vector<KeyMatcher>* out_regex,
                               std::vector<std::string>* warnings) {
  if (!out_fast || !out_regex) {
    return;
  }
  auto items = Split(value, ',');
  for (const auto& item : items) {
    std::string v = Trim(item);
    if (v.empty()) {
      continue;
    }
    KeyMatcher matcher;
    std::string error;
    if (!ParseKeyMatcher(v, &matcher, &error)) {
      if (warnings && !error.empty()) {
        warnings->push_back("key pattern '" + v + "' skipped: " + error);
      }
      continue;
    }
    if (matcher.kind == KeyMatcher::Kind::kRegex) {
      out_regex->push_back(std::move(matcher));
    } else {
      out_fast->push_back(std::move(matcher));
    }
  }
}

bool EventFilter::ParseKeyMatcher(const std::string& value, KeyMatcher* out, std::string* error) {
  if (!out) {
    return false;
  }
  std::string trimmed = Trim(value);
  if (trimmed.empty()) {
    if (error) {
      *error = "empty key pattern";
    }
    return false;
  }

  std::string_view view(trimmed);
  if (view.back() == '*') {
    std::string_view prefix = view.substr(0, view.size() - 1);
    if (!HasRegexMeta(prefix)) {
      out->kind = KeyMatcher::Kind::kPrefix;
      out->raw = trimmed;
      out->text = std::string(prefix);
      return true;
    }
  }

  if (!HasRegexMeta(view)) {
    out->kind = KeyMatcher::Kind::kExact;
    out->raw = trimmed;
    out->text = trimmed;
    return true;
  }

  try {
    out->kind = KeyMatcher::Kind::kRegex;
    out->raw = trimmed;
    out->regex = std::regex(trimmed, std::regex::ECMAScript);
    return true;
  } catch (const std::regex_error& ex) {
    if (error) {
      *error = ex.what();
    }
    return false;
  }
}

std::string EventFilter::Trim(std::string_view value) {
  size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return std::string(value.substr(start, end - start));
}

std::vector<std::string> EventFilter::Split(std::string_view value, char delim) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= value.size()) {
    size_t pos = value.find(delim, start);
    if (pos == std::string_view::npos) {
      pos = value.size();
    }
    out.emplace_back(value.substr(start, pos - start));
    if (pos == value.size()) {
      break;
    }
    start = pos + 1;
  }
  return out;
}

std::string EventFilter::ToLower(std::string_view value) {
  std::string out;
  out.reserve(value.size());
  for (char ch : value) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

bool EventFilter::IsLowerAscii(std::string_view value) {
  for (char ch : value) {
    if (ch >= 'A' && ch <= 'Z') {
      return false;
    }
  }
  return true;
}

bool EventFilter::EqualsIgnoreCase(std::string_view value, std::string_view lower_value) {
  if (value.size() != lower_value.size()) {
    return false;
  }
  for (size_t i = 0; i < value.size(); ++i) {
    char lhs = static_cast<char>(std::tolower(static_cast<unsigned char>(value[i])));
    if (lhs != lower_value[i]) {
      return false;
    }
  }
  return true;
}

bool EventFilter::IsUnknown(std::string_view value) {
  return EqualsIgnoreCase(value, "unknown");
}

bool EventFilter::MatchKeys(std::string_view key,
                            const std::vector<KeyMatcher>& fast,
                            const std::vector<KeyMatcher>& regex) {
  for (const auto& matcher : fast) {
    if (matcher.Match(key)) {
      return true;
    }
  }
  for (const auto& matcher : regex) {
    if (matcher.Match(key)) {
      return true;
    }
  }
  return false;
}

bool EventFilter::MatchValues(std::string_view value,
                              const std::vector<std::string>& values,
                              bool allow_unknown) {
  if (values.empty()) {
    return true;
  }
  if (value.empty()) {
    return true;
  }
  if (allow_unknown && IsUnknown(value)) {
    return true;
  }
  if (IsLowerAscii(value)) {
    for (const auto& candidate : values) {
      if (value == candidate) {
        return true;
      }
    }
    return false;
  }
  for (const auto& candidate : values) {
    if (EqualsIgnoreCase(value, candidate)) {
      return true;
    }
  }
  return false;
}
