FROM ruby:2.6.5-alpine

RUN apk add --no-cache yarn sqlite mariadb-connector-c tzdata mysql-client
RUN gem update bundler

ADD src/Gemfile /src/Gemfile
WORKDIR /src
ENV PATH /src/bin:$PATH

RUN \
	apk add --no-cache --virtual .build-deps build-base sqlite-dev mariadb-connector-c-dev && \
	bundle install -j"$(cat /proc/cpuinfo | grep 'cpu cores' | head -n1 | cut -d: -f2 | awk '{print $1}')" --path=vendor/bundle && \
	apk del .build-deps && \
	rm -Rvf vendor/bundle/ruby/*/cache/*.gem && \
	find vendor/bundle/ruby/*/gems -name '*.c' -or -name '*.o' -print -delete

ADD src/app src/babel.config.js src/config src/package.json src/yarn.lock src/postcss.config.js /src/
RUN yarn install --check-files

ADD src /src

ENV RAILS_ENV=production
ENV DISABLE_DATABASE_ENVIRONMENT_CHECK=1
ADD run.sh /
ADD rails-guide/* /src/

ENTRYPOINT ["/run.sh"]
