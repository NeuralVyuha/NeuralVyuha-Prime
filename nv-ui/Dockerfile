FROM caddy:2.7-alpine

# Use 'app' source and 'bower_components' directly for dev/rapid iteration
WORKDIR /srv
COPY app .
COPY bower_components ./bower_components
COPY Caddyfile /etc/caddy/Caddyfile

EXPOSE 80
EXPOSE 443

