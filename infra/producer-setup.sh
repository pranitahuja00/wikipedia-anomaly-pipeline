#!/bin/bash
set -e

echo "=== Wikipedia Producer Setup ==="

# System updates and Python deps
sudo dnf update -y
sudo dnf install -y python3 python3-pip git

# Clone the repo
cd /home/ec2-user
git clone https://github.com/pranitahuja00/wikipedia-anomaly-pipeline.git
cd wikipedia-anomaly-pipeline

# Install Python dependencies
pip3 install -r requirements.txt

# Create .env file with Kafka credentials
echo ""
echo "=== Enter your Confluent Cloud credentials ==="
read -p "KAFKA_BOOTSTRAP_SERVER: " kafka_bootstrap
read -p "KAFKA_API_KEY: "          kafka_api_key
read -s -p "KAFKA_API_SECRET: "    kafka_api_secret
echo ""

cat > .env <<EOF
KAFKA_BOOTSTRAP_SERVER=${kafka_bootstrap}
KAFKA_API_KEY=${kafka_api_key}
KAFKA_API_SECRET=${kafka_api_secret}
EOF

chmod 600 .env
echo ".env created."

# Install and start systemd service
sudo cp infra/wiki-producer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable wiki-producer
sudo systemctl start wiki-producer

echo ""
echo "=== Setup complete ==="
echo "Check status : sudo systemctl status wiki-producer"
echo "View logs    : sudo journalctl -u wiki-producer -f"
