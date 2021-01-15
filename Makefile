dev-up:
	docker-compose -f docker-compose.dev.yml up -d 

dev-down:
	docker-compose -f docker-compose.dev.yml down 

dev-restart:
	docker-compose -f docker-compose.dev.yml restart 

dev:
	docker-compose -f docker-compose.dev.yml
