FROM ruby:2.7.5

RUN curl -sL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get update -qq && apt-get install -y build-essential libpq-dev nodejs
RUN gem install bundler:2.2.24

RUN mkdir /solidus
WORKDIR /solidus

ADD Gemfile Gemfile.lock /solidus/
RUN bundle install

ADD app /solidus/app
ADD bin /solidus/bin
ADD config /solidus/config
ADD db /solidus/db
ADD lib /solidus/lib
ADD public /solidus/public
ADD test /solidus/test
ADD vendor /solidus/vendor
ADD Rakefile /solidus/Rakefile
ADD config.ru /solidus/config.ru
ADD docker-entrypoint.sh /solidus/docker-entrypoint.sh

EXPOSE 3000
EXPOSE 4000

VOLUME /state

CMD ["bash", "./docker-entrypoint.sh"]
