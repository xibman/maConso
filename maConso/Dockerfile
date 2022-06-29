FROM node

ENV FIRST_RUN_AGE=63072000
ENV TZ=Europe/Paris

WORKDIR /usr/src/app
COPY . .
RUN npm install
RUN npm run build
CMD [ "node", "index.js" ]