const dotenvExpand = require('dotenv-expand');
const dotenv = require('dotenv');
const wait_on = require('wait-on');
const { execSync } = require('child_process');
const { renameSync, readFileSync, writeFileSync } = require('fs');

dotenvExpand.expand(dotenv.config());

if (process.arch === 'arm64') {
  process.env.LOCAL_DB_TYPE = `amd64/${process.env.LOCAL_DB_TYPE}`;
}

// Create wp-config.php.
wp_cli('config create --dbuser=root --dbpass=password --dbname=wordpress_develop --dbhost=db:3333 --path=/var/www/src --force');

// Add the debug settings to wp-config.php.
// Windows requires this to be done as an additional step, rather than using the --extra-php option in the previous step.
wp_cli(`config set WP_DEBUG ${process.env.LOCAL_WP_DEBUG} --raw --type=constant`);
wp_cli(`config set WP_DEBUG_LOG ${process.env.LOCAL_WP_DEBUG_LOG} --raw --type=constant`);
wp_cli(`config set WP_DEBUG_DISPLAY ${process.env.LOCAL_WP_DEBUG_DISPLAY} --raw --type=constant`);
wp_cli(`config set SCRIPT_DEBUG ${process.env.LOCAL_SCRIPT_DEBUG} --raw --type=constant`);
wp_cli(`config set WP_ENVIRONMENT_TYPE ${process.env.LOCAL_WP_ENVIRONMENT_TYPE} --type=constant`);

// Move wp-config.php to the base directory, so it doesn't get mixed up in the src or build directories.
renameSync('src/wp-config.php', 'wp-config.php');

install_wp_importer();

// Read in wp-tests-config-sample.php, edit it to work with our config, then write it to wp-tests-config.php.
const testConfig = readFileSync('wp-tests-config-sample.php', 'utf8')
  .replace('youremptytestdbnamehere', 'wordpress_develop_tests')
  .replace('yourusernamehere', 'root')
  .replace('yourpasswordhere', 'password')
  .replace('localhost', 'db:3333')
  .concat("\ndefine( 'FS_METHOD', 'direct' );\n");

writeFileSync('wp-tests-config.php', testConfig);

// Once the site is available, install WordPress!
wait_on({ resources: [`tcp:localhost:${process.env.LOCAL_PORT}`] })
  .then(() => {
    wp_cli(`core install --title="WordPress Develop" --admin_user=admin --admin_password=password --admin_email=test@test.com --skip-email --url=http://localhost:${process.env.LOCAL_PORT}`);
  });

/**
 * Runs WP-CLI commands in the Docker environment.
 *
 * @param {string} cmd The WP-CLI command to run.
 */
function wp_cli(cmd) {
  execSync(`docker-compose run --rm cli ${cmd}`, { stdio: 'inherit' });
}

/**
 * Downloads the WordPress Importer plugin for use in tests.
 */
function install_wp_importer() {
  const testPluginDirectory = 'tests/phpunit/data/plugins/wordpress-importer';

  execSync(`docker-compose exec -T php rm -rf ${testPluginDirectory}`, { stdio: 'inherit' });
  execSync(`docker-compose exec -T php git clone https://github.com/WordPress/wordpress-importer.git ${testPluginDirectory} --depth=1`, { stdio: 'inherit' });
}
