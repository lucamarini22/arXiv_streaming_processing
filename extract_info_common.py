import cate_map


def extract_func(producer, topic, entry, nltk_words):
    cate_dict = cate_map.cate_dict



    arxiv_id = entry.id.split('/abs/')[-1]
    print('arxiv-id: %s' % arxiv_id)

    title = entry.title
    print('Title:  %s' % title)

    isVersionOne = entry.updated == entry.published
    print('Is first version:  %i' % isVersionOne)

    published_year, published_month, published_day = list(map(int, entry.published.split('T')[0].split('-')))
    print('Published year, month, date:  %i %i %i' % (published_year, published_month, published_day))

    # feedparser v4.1 only grabs the first author
    first_author = entry.author
    print('First Author:  %s' % first_author)
    
    try:
        pageNum = int(entry.arxiv_comment.split('pages')[0])
    except:
        pageNum = -1
    print('Number of pages:  %i' % pageNum)

    # figNum = int(entry.arxiv_comment.split('pages,')[1].split('figure')[0])
    # print('Number of pages:  %i' % pageNum)

    # get all the categories
    all_categories = [t['term'] for t in entry.tags]
    # all_categories = (', ').join(all_categories)
    print('All Categories: %s' % (', ').join(all_categories))

    
    # main category best guess
    main_cate_guess = "Other"
    human_readable_cate = "Other"
    human_readable_main_cate_guess = "Other"
    try:
        main_cate_guess = all_categories[0].split('.')[0]
        if cate_dict.get(main_cate_guess) is not None:
            human_readable_main_cate_guess = cate_dict.get(main_cate_guess)

        human_readable_cate = []
        for idx, cat in enumerate(all_categories):
            # exclude categories that starts with a digit
            if not cat[0].isdigit():
                if cate_dict.get(all_categories[idx]) is not None:
                    human_readable_cate.append(cate_dict.get(all_categories[idx]))
        
    except:
        pass
    print('Main category: %s' % main_cate_guess)
    print('Human readable subcategories: %s' % human_readable_cate)
    
    abstract = entry.summary
    print(abstract)

    common = set(title.split()) & set(abstract.split())
    filtered_common = [word for word in common if word not in nltk_words]

    print(filtered_common)
    
    #print('_' * 40)
    
    producer.send(topic, 
        {
        'key': arxiv_id, 
        'title': title,
        'isVersionOne': isVersionOne,
        'published_year': published_year,
        'published_month': published_month,
        'published_day': published_day,
        'first_author': first_author,
        'page_num': pageNum,
        'categories': all_categories,
        'main_category': main_cate_guess,
        'human_readable_categories': human_readable_cate,
        'human_readable_main_category': human_readable_main_cate_guess,
        'abstract': abstract,
        'keywords': filtered_common
        }
    )