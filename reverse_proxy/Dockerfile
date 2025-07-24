FROM nginx:1.26.2-alpine

# Build argument to determine which config to use
ARG BUILD_ENV=dev

# Copy the appropriate nginx config based on build environment
COPY nginx_${BUILD_ENV}.conf /etc/nginx/nginx.conf

# Expose port 80
EXPOSE 80

# Start nginx
CMD ["nginx", "-g", "daemon off;"]