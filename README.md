# kafka-producer-application

# 카프카 프로듀서 애플리케이션 개발 소스입니다.
## BasicProducer.class
메시지 키가 없고 메시지 값만 있는 레코드 produce

## KeyProducer.class
메시지 키와 값이 존재하는 레코드 produce

## KeyProducerUseCustomPartitioner
레코드가 들어갈 파티션을 지정하는 파티셔너를 직접 제작하여 레코드 produce
