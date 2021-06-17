FROM ruby:2.6.3
RUN curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update -qq && apt-get install -y build-essential libpq-dev nodejs postgresql-client yarn
RUN mkdir /sample_rails_application
WORKDIR /sample_rails_application
COPY Gemfile /sample_rails_application/Gemfile
COPY Gemfile.lock /sample_rails_application/Gemfile.lock
COPY package.json /sample_rails_application/package.json
COPY yarn.lock /sample_rails_application/yarn.lock
RUN gem install bundler -v '2.0.2'
RUN bundle install
RUN yarn install --check-files
COPY . /sample_rails_application
EXPOSE 3000

#CMD ["rails", "server", "-b", "0.0.0.0"]