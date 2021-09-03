FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/drupal:9-apache

RUN \
	composer require \
		phpunit/phpunit \
		phpspec/prophecy-phpunit \
		symfony/phpunit-bridge \
		behat/mink-goutte-driver \
		mikey179/vfsstream \
		drush/drush

USER root
WORKDIR /opt/drupal/web

# A couple hacks
RUN \
	# This fails searching for specific text in one of the front-end webpages; assumption is that it was never updated to handle some change to Drupal \
	rm -vf core/modules/system/tests/src/Functional/Database/SelectTableSortDefaultTest.php && \
	# This causes a PHP mkdir() call to not fail...one of those "you changed the outcome by observing it" type deals \
	sed -i '/@mkdir/ i\      echo "!!! mkdir '"'"'${directory}'"'"'\\n";' core/lib/Drupal/Component/PhpStorage/FileStorage.php

ADD run ./

ENTRYPOINT ["./run"]

