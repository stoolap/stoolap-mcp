# Builds the Stoolap MCP server from this repository.
# The server speaks MCP over stdio; run it with -i so the client can talk to it:
#   docker build -t stoolap-mcp .
#   docker run -i --rm -v "$PWD/data:/data" stoolap-mcp --path /data/mydb
FROM node:22-slim AS build
# @stoolap/node compiles its small N-API glue with node-gyp at install time,
# so the build stage needs Python and a C++ toolchain. The Rust engine itself
# arrives prebuilt through the @stoolap/lib-* optional packages.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 make g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npm run build && npm prune --omit=dev

FROM node:22-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/build ./build
ENTRYPOINT ["node", "build/index.js"]
