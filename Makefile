# Makefile para gerenciar Docker Compose

.PHONY: start stop restart status enter-db

# Iniciar os serviços em background
start:
	docker-compose up -d

# Parar os serviços
stop:
	docker-compose down

# Reiniciar os serviços
restart:
	docker-compose restart

# Ver o status dos serviços
status:
	docker-compose ps

# Entrar no banco de dados MySQL como usuário admin
enter-db:
	docker-compose exec warehouse mysql -u admin -p

start-main-db:
	docker start mysql-container

stop-main-db:
	docker stop mysql-container

exec-main-db:
	docker exec -it mysql-container mysql -u admin -p