#include "interceptor/file.hh"
namespace nextfs {
// auto resolve_pathname(const char *pathname) -> std::pair<uint8_t,
// std::string> {
//   if (unlikely(pathname == nullptr)) {
//     return std::pair(0, "");
//   }
//   std::string tmp;
//   if (pathname[0] == '/') {
//     tmp = pathname;
//   } else {
//     size_t raw_len = strlen(pathname);
//     tmp.reserve(+raw_len + 1);
//     tmp.push_back('/');
//     tmp.append(pathname, raw_len);
//   }
//   std::string result;
//   result.reserve(tmp.size());
//   uint8_t num = 0;
//   for (size_t n_pos = 0, last_slash_pos = 0; n_pos < tmp.size(); n_pos++) {
//     size_t start = n_pos;
//     // skip continuous slashes
//     while (start < tmp.size() && tmp[start] == '/') {
//       start++;
//     }
//     // next slash
//     n_pos = tmp.find('/', start);
//     if (n_pos == std::string::npos) {
//       n_pos = tmp.size();
//     }

//     size_t name_len = n_pos - start;
//     if (name_len == 0) {
//       break;
//     }
//     if (name_len == 1 && tmp[start] == '.') {
//       continue;
//     }
//     if (name_len == 2 && tmp[start] == '.' && tmp[start + 1] == '.') {
//       if (!result.empty()) {
//         result.erase(last_slash_pos);
//         last_slash_pos = result.find_last_of('/');
//         num--;
//       }
//       continue;
//     }
//     last_slash_pos = result.size();
//     result.push_back('/');
//     result.append(tmp, start, name_len);
//     num++;
//   }
//   if (result.empty()) {
//     result.push_back('/');
//   }
//   return std::pair(num, result);
// }
}; // namespace nextfs