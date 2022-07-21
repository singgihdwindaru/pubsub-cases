mockfile:
	@mockgen -package=mocks -source=src/repo/pubsubRepo.go -destination=src/mocks/pubsubRepo_mock.go
	@mockgen -package=mocks -source=src/services/pubsubService.go -destination=src/mocks/pubsubService_mock.go
