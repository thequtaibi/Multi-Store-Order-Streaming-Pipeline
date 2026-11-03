FROM ubuntu
COPY /home/kind/Desktop/project_x
RUN apt-get update
RUN apt-ger -y python3
RUN source .venv/bin/activate
RUN python -m store_simulation
