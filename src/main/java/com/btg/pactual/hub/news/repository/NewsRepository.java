package com.btg.pactual.hub.news.repository;
import com.btg.pactual.hub.news.model.NewsDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.Optional;
public interface NewsRepository extends MongoRepository<NewsDocument, String> {
  Optional<NewsDocument> findByIdEquals(String id);
}
