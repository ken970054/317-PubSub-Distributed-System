# 317-PubSub-Distributed-System

## Project Idea
Our goal for this project is to design a system for e-commerce companies to manage their order, packaging, shipping, and notification service separately in different containers as an eventdriven distributed system. First, we will use **Golang**, an efficient and accessible language for a distributed system, to build the basic pub-sub system. The second is to design topics in the broker for subsystems to send/receive events correctly. For deployment, we use Docker containers to maintain five subsystems (order + package + shipping + notification + broker). Also, we will use Docker Compose to define and run multi-containers.

To be modified
(The event of event-driven architecture is a status change or update. We could implement a loosely-coupled system by using the event to set up the exchange of information between two entities. **Distributed MQTT (TD-MQTT) Brokers** can implement in our project; the transparency will help to use data without worrying about their location and dynamic configuration changes. Another algorithm we will implement is the **Gossip protocol**. It can improve the reliability of our system. The last algorithm we will include is for concurrency control. We can use the built-in package in the Go language to ensure concurrency. An extra one we had done some research on is using **Skip Graph** to handle exhaust data for achieving high scalability.

For the last step, we will test the functions first, going through the scenario of user ordering flow and how those services will send events. After that, we will compare the performance with and without our algorithms.)

## Syetem Structure

## Golang with Docker Depolyment
