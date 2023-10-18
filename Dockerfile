# Use an official Node.js runtime as the base image
FROM node:18

# Set the working directory in the container
WORKDIR /app

RUN git clone https://github.com/dlobregon/amm_file_downloader.git

# Install backend dependencies
WORKDIR /app/amm_file_downloader
# install dependencies
RUN npm install
# run script to donwnload excel files
RUN npm run download 
# run script to load database
RUN npm run loadExcel 
# run script to generate report table
RUN npm run compute

EXPOSE 4000
#EXPOSE 8080

# Command to start both the backend 
CMD ["npm", "start"] 
