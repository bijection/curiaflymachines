
FROM node:20-slim

# Install system dependencies
# curl: needed to install flyctl
# ca-certificates: needed for HTTPS requests
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install flyctl
RUN curl -L https://fly.io/install.sh | sh

# Add flyctl to PATH
ENV FLYCTL_INSTALL="/root/.fly"
ENV PATH="$FLYCTL_INSTALL/bin:$PATH"

WORKDIR /app

# Install Node dependencies
COPY package*.json ./
RUN npm install --production

# Copy application source
COPY . .

# Set environment defaults
ENV PORT=3000
EXPOSE 3000

CMD ["npm", "start"]
