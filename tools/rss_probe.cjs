// Report OS-maintained process high-water RSS; payload accounting is separate.
const path = require('node:path');
const entry = path.resolve(process.argv[2]);
process.argv.splice(1, 1);
process.on('exit', () => {
  const usage = process.resourceUsage();
  process.stderr.write('MOONMCAP_RSS=' + JSON.stringify({
    max_rss_kib: usage.maxRSS,
    final_rss_bytes: process.memoryUsage().rss,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
  }) + '\n');
});
require(entry);
